package com.example.flinkiceberg;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.JdbcLockFactory;
import org.apache.iceberg.flink.maintenance.api.TriggerLockFactory;

/**
 * Shared Iceberg catalog/lock wiring used by the two Flink <b>maintenance</b>
 * jobs in this demo: {@link IcebergMaintenanceJob} (rewrite + expire) and
 * {@link IcebergOrphanGcJob} (orphan-file removal).
 *
 * <p>Ingestion no longer runs on Flink — it is handled by the Apache Iceberg
 * <b>Kafka Connect</b> sink connector (see {@code k8s/kafka-connect.yaml} and
 * {@code k8s/connector-setup.yaml}), which reads JSON-Schema records from
 * Schema Registry and <em>owns</em> the table: it auto-creates {@code db.events}
 * and evolves its schema from the registry. The table schema and partition spec
 * therefore live in the connector config + the registered JSON Schema, not here.
 *
 * <p>The two maintenance jobs are deployed as separate Application-Mode clusters
 * so they have independent lifecycles, failure domains and resource budgets;
 * this class keeps the parts they must agree on — env-driven catalog config and
 * the Postgres-backed maintenance lock wiring — in one place. Both share the
 * same {@code db.events} lock id, so the lock serialises orphan GC against
 * expire/rewrite.
 *
 * <p>All settings are environment-driven so the same jar runs locally and
 * inside the minikube/Application-Mode deployment, and so they match the
 * {@code iceberg.catalog.*} properties given to the Kafka Connect sink.
 */
final class IcebergCatalog {

  private final String catalogUri;
  private final String jdbcUser;
  private final String jdbcPassword;
  private final String dbName;
  private final String tableName;
  private final CatalogLoader catalogLoader;
  private final TableIdentifier tableId;

  private IcebergCatalog(
      String catalogUri,
      String jdbcUser,
      String jdbcPassword,
      String dbName,
      String tableName,
      CatalogLoader catalogLoader,
      TableIdentifier tableId) {
    this.catalogUri = catalogUri;
    this.jdbcUser = jdbcUser;
    this.jdbcPassword = jdbcPassword;
    this.dbName = dbName;
    this.tableName = tableName;
    this.catalogLoader = catalogLoader;
    this.tableId = tableId;
  }

  /** Build the catalog wiring from environment variables (container-friendly defaults). */
  static IcebergCatalog fromEnv() {
    final String catalogUri = env("ICEBERG_JDBC_URI", "jdbc:postgresql://postgres:5432/iceberg");
    final String jdbcUser = env("ICEBERG_JDBC_USER", "iceberg");
    final String jdbcPassword = env("ICEBERG_JDBC_PASSWORD", "iceberg");
    final String warehouse = env("ICEBERG_WAREHOUSE", "s3://warehouse");
    final String s3Endpoint = env("S3_ENDPOINT", "http://minio:9000");
    final String s3AccessKey = env("S3_ACCESS_KEY", "admin");
    final String s3SecretKey = env("S3_SECRET_KEY", "password");
    final String s3Region = env("S3_REGION", "us-east-1");
    final String dbName = env("ICEBERG_DB", "db");
    final String tableName = env("ICEBERG_TABLE", "events");

    Map<String, String> catalogProps = new HashMap<>();
    catalogProps.put("catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog");
    catalogProps.put("uri", catalogUri);
    catalogProps.put("jdbc.user", jdbcUser);
    catalogProps.put("jdbc.password", jdbcPassword);
    catalogProps.put("warehouse", warehouse);
    catalogProps.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
    catalogProps.put("s3.endpoint", s3Endpoint);
    catalogProps.put("s3.path-style-access", "true");
    catalogProps.put("s3.access-key-id", s3AccessKey);
    catalogProps.put("s3.secret-access-key", s3SecretKey);
    catalogProps.put("client.region", s3Region);

    Configuration hadoopConf = new Configuration(false);
    CatalogLoader catalogLoader =
        CatalogLoader.custom(
            "demo", catalogProps, hadoopConf, "org.apache.iceberg.jdbc.JdbcCatalog");

    return new IcebergCatalog(
        catalogUri,
        jdbcUser,
        jdbcPassword,
        dbName,
        tableName,
        catalogLoader,
        TableIdentifier.of(dbName, tableName));
  }

  CatalogLoader catalogLoader() {
    return catalogLoader;
  }

  TableIdentifier tableId() {
    return tableId;
  }

  TableLoader tableLoader() {
    return TableLoader.fromCatalog(catalogLoader, tableId);
  }

  /**
   * Block until Postgres accepts a JDBC connection, retrying with a fixed
   * delay. In Application Mode each job's {@code main()} runs in its
   * JobManager pod, so this executes there before the job graph is built —
   * a <em>fail-fast pre-flight</em>: if Postgres is genuinely
   * down/misconfigured it surfaces a clear error from the JM pod instead of
   * a cryptic deploy-time job failure.
   *
   * <p>It does <b>not</b> prevent the TaskManager cold-connect race — that
   * connection is made on the TM operator; absorbing that transient is
   * {@link RetryingTriggerLockFactory}'s job.
   *
   * @throws IllegalStateException if still unreachable after {@code attempts}
   */
  void awaitJdbc(int attempts, Duration delay) {
    Properties creds = new Properties();
    creds.setProperty("user", jdbcUser);
    creds.setProperty("password", jdbcPassword);
    try {
      // Belt-and-suspenders: the shaded jar registers the driver via SPI, but
      // force-loading it makes a missing driver fail clearly here.
      Class.forName("org.postgresql.Driver");
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("PostgreSQL JDBC driver not on classpath", e);
    }
    Exception last = null;
    for (int attempt = 1; attempt <= attempts; attempt++) {
      try (Connection c = DriverManager.getConnection(catalogUri, creds)) {
        if (c.isValid(5)) {
          return;
        }
        last = new IllegalStateException("connection reported invalid");
      } catch (Exception e) {
        last = e;
      }
      try {
        Thread.sleep(delay.toMillis());
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException("Interrupted while awaiting Postgres", ie);
      }
    }
    throw new IllegalStateException(
        "Postgres not reachable at " + catalogUri + " after " + attempts + " attempts", last);
  }

  /**
   * Lock used by the maintenance graph so compaction, snapshot expiration and
   * orphan cleanup coordinate their triggers. It is backed by the *same*
   * Postgres as the JDBC catalog. The Kafka Connect ingest does not use this
   * lock; it commits via its own Iceberg-commit coordinator. JdbcLockFactory
   * auto-creates its lock table.
   *
   * <p>Wrapped in {@link RetryingTriggerLockFactory}: {@code open()} runs on
   * the TaskManager operator and its first JDBC connection can lose a
   * cold-connect race on deploy; retrying it in-place absorbs that transient
   * without restarting the whole job (a client-side pre-flight can't, since
   * the failing connection is made on the TaskManager).
   */
  TriggerLockFactory lockFactory() {
    JdbcLockFactory jdbc =
        new JdbcLockFactory(
            catalogUri,
            dbName + "." + tableName,
            Map.of(
                "jdbc.user", jdbcUser,
                "jdbc.password", jdbcPassword,
                JdbcLockFactory.INIT_LOCK_TABLES_PROPERTY, "true"));
    return new RetryingTriggerLockFactory(jdbc, 5, 3_000L);
  }

  static String env(String key, String defaultValue) {
    String v = System.getenv(key);
    return (v == null || v.isBlank()) ? defaultValue : v;
  }
}
