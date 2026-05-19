package com.example.flinkiceberg;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.JdbcLockFactory;
import org.apache.iceberg.flink.maintenance.api.TriggerLockFactory;
import org.apache.iceberg.types.Types;

/**
 * Shared Iceberg catalog/table wiring used by both Flink jobs in this demo:
 * the {@link KafkaToIcebergJob} ingest job and the {@link IcebergMaintenanceJob}
 * maintenance job.
 *
 * <p>The two jobs are deployed separately so ingest and maintenance have
 * independent lifecycles and failure domains; this class keeps the parts they
 * must agree on — schema, env-driven catalog config, and the Postgres-backed
 * maintenance lock — in one place.
 *
 * <p>All settings are environment-driven so the same jar runs locally and in
 * the Docker Compose stack.
 */
final class IcebergCatalog {

  // Iceberg table schema. Field order here is the order produced by the
  // JSON -> RowData mapper and expected by the Iceberg sink; JsonToRowData
  // must stay in lockstep with it.
  static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.StringType.get()),
          Types.NestedField.optional(2, "event_type", Types.StringType.get()),
          Types.NestedField.optional(3, "user_id", Types.StringType.get()),
          Types.NestedField.optional(4, "amount", Types.DoubleType.get()),
          Types.NestedField.optional(5, "event_time", Types.TimestampType.withZone()));

  // Identity-partition by event_type so the demo produces many small data
  // files that RewriteDataFiles can later compact.
  static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("event_type").build();

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
   * delay. Run in the Flink client JVM (the {@code submitter}) before the job
   * graph is built, this is a <em>fail-fast pre-flight</em>: if Postgres is
   * genuinely down/misconfigured it surfaces a clear error from the submitter
   * before any job is created, instead of a cryptic deploy-time job failure.
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
   * Idempotently ensure the namespace and table exist. Both jobs call this so
   * either can be deployed first; concurrent creation (e.g. both submitted at
   * once) is tolerated by swallowing {@link AlreadyExistsException}. Runs in
   * the Flink client JVM (the {@code submitter} container).
   */
  void ensureTable() {
    // org.apache.iceberg.catalog.Catalog is not AutoCloseable; close manually.
    Catalog catalog = catalogLoader.loadCatalog();
    try {
      Namespace ns = Namespace.of(dbName);
      if (catalog instanceof SupportsNamespaces nsCatalog && !nsCatalog.namespaceExists(ns)) {
        try {
          nsCatalog.createNamespace(ns);
        } catch (AlreadyExistsException raced) {
          // another job/process created it first — fine
        }
      }
      if (!catalog.tableExists(tableId)) {
        try {
          catalog.createTable(tableId, SCHEMA, SPEC, Map.of("format-version", "2"));
        } catch (AlreadyExistsException raced) {
          // another job/process created it first — fine
        }
      }
    } catch (Exception e) {
      throw new IllegalStateException("Failed to ensure Iceberg table exists", e);
    } finally {
      if (catalog instanceof Closeable closeable) {
        try {
          closeable.close();
        } catch (IOException ignored) {
          // best-effort close of the client-side catalog
        }
      }
    }
  }

  /**
   * Lock shared by maintenance tasks (and any other writer) so compaction and
   * snapshot expiration never collide. It is backed by the *same* Postgres as
   * the JDBC catalog, so the lock is honoured across the separately-deployed
   * ingest and maintenance jobs. JdbcLockFactory auto-creates its lock table.
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
