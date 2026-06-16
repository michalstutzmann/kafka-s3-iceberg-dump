package com.example.sparkiceberg;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Serialise the two Spark maintenance jobs with a Kubernetes {@code Lease}
 * (coordination.k8s.io/v1) instead of a database lock, so the Spark side talks
 * only to Polaris and the Kubernetes API — never directly to Postgres.
 *
 * <p>A single Lease (default name {@code iceberg-maintenance}) acts as the mutex:
 * rewrite/expire and orphan-GC each acquire it before running, so an overlapping
 * cron schedule serialises them. The holder identity is the pod name; a crashed
 * holder is reclaimed once the lease's {@code leaseDurationSeconds} elapses past
 * its last {@code renewTime}, so a job that dies without releasing cannot wedge
 * the other forever.
 *
 * <p>Talks to the in-cluster API server with the pod's mounted ServiceAccount
 * token + CA, using only the JDK HTTP client and Jackson (already on the Spark
 * runtime classpath) — no Kubernetes client dependency is bundled.
 */
final class KubernetesLease {

  private static final String SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount";
  private static final String GROUP = "/apis/coordination.k8s.io/v1/namespaces/";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final DateTimeFormatter MICROS =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'");

  private final HttpClient http;
  private final String apiBase;
  private final String token;
  private final String namespace;
  private final String leaseName;
  private final String holder;
  private final long leaseSeconds;
  private final Duration acquireTimeout;
  private final Duration pollInterval;

  private KubernetesLease(
      HttpClient http,
      String apiBase,
      String token,
      String namespace,
      String leaseName,
      String holder,
      long leaseSeconds,
      Duration acquireTimeout,
      Duration pollInterval) {
    this.http = http;
    this.apiBase = apiBase;
    this.token = token;
    this.namespace = namespace;
    this.leaseName = leaseName;
    this.holder = holder;
    this.leaseSeconds = leaseSeconds;
    this.acquireTimeout = acquireTimeout;
    this.pollInterval = pollInterval;
  }

  /** Build from the in-cluster environment (ServiceAccount mount + env vars). */
  static KubernetesLease fromEnv() {
    try {
      String host = requireEnv("KUBERNETES_SERVICE_HOST");
      String port = IcebergCatalog.env("KUBERNETES_SERVICE_PORT", "443");
      String token = Files.readString(Path.of(SA_DIR, "token")).trim();
      String namespace =
          IcebergCatalog.env("POD_NAMESPACE", readOrNull(Path.of(SA_DIR, "namespace")));
      String holder = IcebergCatalog.env("POD_NAME", hostname());
      String leaseName = IcebergCatalog.env("MAINTENANCE_LEASE_NAME", "iceberg-maintenance");
      long leaseSeconds = Long.parseLong(IcebergCatalog.env("MAINTENANCE_LEASE_SECONDS", "600"));
      long acquireSeconds =
          Long.parseLong(IcebergCatalog.env("MAINTENANCE_LEASE_ACQUIRE_SECONDS", "300"));

      HttpClient client =
          HttpClient.newBuilder()
              .sslContext(trustSaCa())
              .connectTimeout(Duration.ofSeconds(10))
              .build();

      return new KubernetesLease(
          client,
          "https://" + host + ":" + port,
          token,
          namespace,
          leaseName,
          holder,
          leaseSeconds,
          Duration.ofSeconds(acquireSeconds),
          Duration.ofSeconds(5));
    } catch (Exception e) {
      throw new IllegalStateException("Failed to initialise Kubernetes lease client", e);
    }
  }

  /**
   * Acquire the lease (waiting up to the configured timeout), run {@code body},
   * then release it. The lease is released in a finally block so the sibling job
   * can proceed promptly on the normal path.
   */
  void withLock(IcebergCatalog.CheckedRunnable body) throws Exception {
    String resourceVersion = acquire();
    try {
      body.run();
    } finally {
      release(resourceVersion);
    }
  }

  /** Block until we hold the lease; returns the held object's resourceVersion. */
  private String acquire() throws Exception {
    Instant deadline = Instant.now().plus(acquireTimeout);
    while (true) {
      HttpResponse<String> get = send("GET", leaseUri(), null);
      if (get.statusCode() == 404) {
        String rv = tryCreate();
        if (rv != null) {
          return rv;
        }
      } else if (get.statusCode() == 200) {
        JsonNode lease = MAPPER.readTree(get.body());
        if (isFree(lease) || holder.equals(holderOf(lease))) {
          String rv = tryTakeOver(lease);
          if (rv != null) {
            return rv;
          }
        }
      } else {
        throw new IllegalStateException(
            "Unexpected status reading lease " + leaseName + ": " + get.statusCode() + " " + get.body());
      }

      if (Instant.now().isAfter(deadline)) {
        throw new IllegalStateException(
            "Timed out after "
                + acquireTimeout.toSeconds()
                + "s waiting for maintenance lease "
                + leaseName);
      }
      System.out.println("Maintenance lease " + leaseName + " held by another job; waiting...");
      Thread.sleep(pollInterval.toMillis());
    }
  }

  /** POST a new Lease; null if someone else won the race (409). */
  private String tryCreate() throws Exception {
    String body =
        "{\"apiVersion\":\"coordination.k8s.io/v1\",\"kind\":\"Lease\","
            + "\"metadata\":{\"name\":\""
            + leaseName
            + "\",\"namespace\":\""
            + namespace
            + "\"},\"spec\":"
            + spec()
            + "}";
    HttpResponse<String> resp = send("POST", collectionUri(), body);
    if (resp.statusCode() == 201) {
      return MAPPER.readTree(resp.body()).at("/metadata/resourceVersion").asText();
    }
    if (resp.statusCode() == 409) {
      return null;
    }
    throw new IllegalStateException("Failed to create lease: " + resp.statusCode() + " " + resp.body());
  }

  /** PUT to claim an existing (free or self-held) Lease; null on a 409 conflict. */
  private String tryTakeOver(JsonNode current) throws Exception {
    String rv = current.at("/metadata/resourceVersion").asText();
    String body =
        "{\"apiVersion\":\"coordination.k8s.io/v1\",\"kind\":\"Lease\","
            + "\"metadata\":{\"name\":\""
            + leaseName
            + "\",\"namespace\":\""
            + namespace
            + "\",\"resourceVersion\":\""
            + rv
            + "\"},\"spec\":"
            + spec()
            + "}";
    HttpResponse<String> resp = send("PUT", leaseUri(), body);
    if (resp.statusCode() == 200) {
      return MAPPER.readTree(resp.body()).at("/metadata/resourceVersion").asText();
    }
    if (resp.statusCode() == 409) {
      return null; // lost the race; caller re-reads and retries
    }
    throw new IllegalStateException("Failed to acquire lease: " + resp.statusCode() + " " + resp.body());
  }

  /** Clear holder identity so a waiting job can take the lease immediately. */
  private void release(String resourceVersion) {
    try {
      String body =
          "{\"apiVersion\":\"coordination.k8s.io/v1\",\"kind\":\"Lease\","
              + "\"metadata\":{\"name\":\""
              + leaseName
              + "\",\"namespace\":\""
              + namespace
              + "\",\"resourceVersion\":\""
              + resourceVersion
              + "\"},\"spec\":{\"leaseDurationSeconds\":"
              + leaseSeconds
              + "}}";
      HttpResponse<String> resp = send("PUT", leaseUri(), body);
      if (resp.statusCode() != 200) {
        // Non-fatal: the lease expires on its own after leaseDurationSeconds.
        System.out.println(
            "Warning: could not release lease " + leaseName + ": " + resp.statusCode() + " " + resp.body());
      }
    } catch (Exception e) {
      System.out.println("Warning: error releasing lease " + leaseName + ": " + e.getMessage());
    }
  }

  private String spec() {
    String now = MICROS.format(Instant.now().atZone(java.time.ZoneOffset.UTC));
    return "{\"holderIdentity\":\""
        + holder
        + "\",\"leaseDurationSeconds\":"
        + leaseSeconds
        + ",\"acquireTime\":\""
        + now
        + "\",\"renewTime\":\""
        + now
        + "\"}";
  }

  private boolean isFree(JsonNode lease) {
    String currentHolder = holderOf(lease);
    if (currentHolder == null || currentHolder.isBlank()) {
      return true;
    }
    JsonNode renew = lease.at("/spec/renewTime");
    JsonNode dur = lease.at("/spec/leaseDurationSeconds");
    if (renew.isMissingNode() || renew.isNull() || dur.isMissingNode() || dur.isNull()) {
      return true;
    }
    Instant expiry = Instant.parse(renew.asText()).plusSeconds(dur.asLong());
    return Instant.now().isAfter(expiry);
  }

  private static String holderOf(JsonNode lease) {
    JsonNode h = lease.at("/spec/holderIdentity");
    return (h.isMissingNode() || h.isNull()) ? null : h.asText();
  }

  private HttpResponse<String> send(String method, URI uri, String body) throws IOException, InterruptedException {
    HttpRequest.BodyPublisher publisher =
        body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body);
    HttpRequest request =
        HttpRequest.newBuilder(uri)
            .timeout(Duration.ofSeconds(15))
            .header("Authorization", "Bearer " + token)
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .method(method, publisher)
            .build();
    return http.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private URI collectionUri() {
    return URI.create(apiBase + GROUP + namespace + "/leases");
  }

  private URI leaseUri() {
    return URI.create(apiBase + GROUP + namespace + "/leases/" + leaseName);
  }

  private static SSLContext trustSaCa() throws Exception {
    try (var in = Files.newInputStream(Path.of(SA_DIR, "ca.crt"))) {
      Collection<? extends Certificate> certs =
          CertificateFactory.getInstance("X.509").generateCertificates(in);
      KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
      ks.load(null, null);
      int i = 0;
      for (Certificate cert : certs) {
        ks.setCertificateEntry("ca-" + i++, cert);
      }
      TrustManagerFactory tmf =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(ks);
      SSLContext ctx = SSLContext.getInstance("TLS");
      ctx.init(null, tmf.getTrustManagers(), null);
      return ctx;
    }
  }

  private static String requireEnv(String key) {
    String v = System.getenv(key);
    if (v == null || v.isBlank()) {
      throw new IllegalStateException(
          "Env " + key + " not set — KubernetesLease must run inside a pod");
    }
    return v;
  }

  private static String readOrNull(Path path) {
    try {
      return Files.readString(path).trim();
    } catch (IOException e) {
      return null;
    }
  }

  private static String hostname() {
    String h = System.getenv("HOSTNAME");
    return (h == null || h.isBlank()) ? "maintenance" : h;
  }
}
