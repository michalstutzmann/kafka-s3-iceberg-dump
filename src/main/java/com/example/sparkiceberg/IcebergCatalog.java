package com.example.sparkiceberg;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.spark.sql.SparkSession;

/**
 * Shared Iceberg catalog and Spark session wiring for the two maintenance jobs.
 *
 * <p>The catalog is Apache Polaris, reached over the Iceberg REST protocol. The
 * same Polaris catalog (name {@code iceberg}) is shared with the Kafka Connect
 * ingest sink, which owns table creation and schema evolution; these Spark jobs
 * only load the existing table and run maintenance actions against it.
 *
 * <p>Polaris vends the MinIO/S3 credentials (the {@code vended-credentials}
 * delegation header), so no S3 keys are configured here. Maintenance runs are
 * serialised with a Kubernetes {@code Lease} ({@link KubernetesLease}) rather
 * than a database lock, so the Spark side never talks directly to Postgres.
 */
final class IcebergCatalog {

  private static final String CATALOG_NAME = "iceberg";
  private static final String REST_IMPL = "org.apache.iceberg.rest.RESTCatalog";

  private final String catalogUri;
  private final String dbName;
  private final String tableName;
  private final TableIdentifier tableId;
  private final Map<String, String> catalogProps;

  private IcebergCatalog(
      String catalogUri,
      String dbName,
      String tableName,
      Map<String, String> catalogProps,
      TableIdentifier tableId) {
    this.catalogUri = catalogUri;
    this.dbName = dbName;
    this.tableName = tableName;
    this.catalogProps = Map.copyOf(catalogProps);
    this.tableId = tableId;
  }

  /** Build catalog wiring from environment variables with container-friendly defaults. */
  static IcebergCatalog fromEnv() {
    final String catalogUri = env("POLARIS_URI", "http://polaris:8181/api/catalog");
    final String warehouse = env("POLARIS_WAREHOUSE", "iceberg");
    final String credential = env("POLARIS_CREDENTIAL", "root:s3cr3t");
    final String scope = env("POLARIS_SCOPE", "PRINCIPAL_ROLE:ALL");
    final String dbName = env("ICEBERG_DB", "db");
    final String tableName = env("ICEBERG_TABLE", "events");

    Map<String, String> catalogProps = new HashMap<>();
    catalogProps.put("uri", catalogUri);
    catalogProps.put("warehouse", warehouse);
    catalogProps.put("credential", credential);
    catalogProps.put("scope", scope);
    // Be explicit about OAuth2 so the REST client does not infer it (and warn);
    // the server URI also future-proofs against Iceberg removing the fallback.
    catalogProps.put("rest.auth.type", "oauth2");
    catalogProps.put("oauth2-server-uri", catalogUri + "/v1/oauth/tokens");
    // Ask Polaris to vend short-lived S3 access for each table load, so the
    // S3FileIO config (endpoint + credentials) comes from the catalog response.
    catalogProps.put("header.X-Iceberg-Access-Delegation", "vended-credentials");

    return new IcebergCatalog(
        catalogUri, dbName, tableName, catalogProps, TableIdentifier.of(dbName, tableName));
  }

  SparkSession sparkSession(String appName) {
    SparkSession.Builder builder =
        SparkSession.builder()
            .appName(appName)
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog." + CATALOG_NAME, "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog." + CATALOG_NAME + ".type", "rest");

    catalogProps.forEach(
        (key, value) -> builder.config("spark.sql.catalog." + CATALOG_NAME + "." + key, value));
    return builder.getOrCreate();
  }

  Table loadTable() {
    Catalog catalog =
        CatalogUtil.loadCatalog(REST_IMPL, CATALOG_NAME, catalogProps, new Configuration(false));
    return catalog.loadTable(tableId);
  }

  String tableName() {
    return dbName + "." + tableName;
  }

  /**
   * Block until the Polaris REST catalog answers, retrying with a fixed delay.
   * Any HTTP response means the server is up; only connection failures are
   * retried, so CronJob logs show a clear startup error if Polaris is genuinely
   * down or misconfigured.
   */
  void awaitRest(int attempts, Duration delay) {
    HttpClient client =
        HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    HttpRequest probe =
        HttpRequest.newBuilder(URI.create(catalogUri + "/v1/config"))
            .timeout(Duration.ofSeconds(5))
            .GET()
            .build();

    Exception last = null;
    for (int attempt = 1; attempt <= attempts; attempt++) {
      try {
        client.send(probe, HttpResponse.BodyHandlers.discarding());
        return; // any HTTP status means Polaris is reachable
      } catch (Exception e) {
        last = e;
      }

      try {
        Thread.sleep(delay.toMillis());
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted while awaiting Polaris", ie);
      }
    }

    throw new IllegalStateException(
        "Polaris not reachable at " + catalogUri + " after " + attempts + " attempts", last);
  }

  /**
   * Serialise maintenance jobs with a Kubernetes Lease keyed on the maintenance
   * lease name. The lease is reclaimed automatically if a holder dies without
   * releasing it (see {@link KubernetesLease}).
   */
  void withMaintenanceLock(CheckedRunnable body) throws Exception {
    KubernetesLease.fromEnv().withLock(body);
  }

  static String env(String key, String defaultValue) {
    String v = System.getenv(key);
    return (v == null || v.isBlank()) ? defaultValue : v;
  }

  @FunctionalInterface
  interface CheckedRunnable {
    void run() throws Exception;
  }
}
