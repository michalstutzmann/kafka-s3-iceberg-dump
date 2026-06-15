package com.example.sparkiceberg;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.spark.sql.SparkSession;

/**
 * Shared Iceberg catalog and Spark session wiring for the two maintenance jobs.
 *
 * <p>Ingestion is still handled by the Apache Iceberg Kafka Connect sink. It
 * owns table creation and schema evolution; these Spark jobs only load the
 * existing table and run maintenance actions against it.
 */
final class IcebergCatalog {

  private static final String CATALOG_NAME = "iceberg";

  private final String catalogUri;
  private final String jdbcUser;
  private final String jdbcPassword;
  private final String dbName;
  private final String tableName;
  private final TableIdentifier tableId;
  private final Map<String, String> catalogProps;

  private IcebergCatalog(
      String catalogUri,
      String jdbcUser,
      String jdbcPassword,
      String dbName,
      String tableName,
      Map<String, String> catalogProps,
      TableIdentifier tableId) {
    this.catalogUri = catalogUri;
    this.jdbcUser = jdbcUser;
    this.jdbcPassword = jdbcPassword;
    this.dbName = dbName;
    this.tableName = tableName;
    this.catalogProps = Map.copyOf(catalogProps);
    this.tableId = tableId;
  }

  /** Build catalog wiring from environment variables with container-friendly defaults. */
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

    return new IcebergCatalog(
        catalogUri,
        jdbcUser,
        jdbcPassword,
        dbName,
        tableName,
        catalogProps,
        TableIdentifier.of(dbName, tableName));
  }

  SparkSession sparkSession(String appName) {
    SparkSession.Builder builder =
        SparkSession.builder()
            .appName(appName)
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog." + CATALOG_NAME, "org.apache.iceberg.spark.SparkCatalog");

    catalogProps.forEach(
        (key, value) -> builder.config("spark.sql.catalog." + CATALOG_NAME + "." + key, value));
    return builder.getOrCreate();
  }

  Table loadTable() {
    Catalog catalog =
        CatalogUtil.loadCatalog(
            "org.apache.iceberg.jdbc.JdbcCatalog",
            CATALOG_NAME,
            catalogProps,
            new Configuration(false));
    return catalog.loadTable(tableId);
  }

  String tableName() {
    return dbName + "." + tableName;
  }

  /**
   * Block until Postgres accepts a JDBC connection, retrying with a fixed delay.
   * This gives CronJob logs a clear startup error if the catalog is genuinely
   * down or misconfigured.
   */
  void awaitJdbc(int attempts, Duration delay) {
    Properties creds = jdbcCredentials();
    loadPostgresDriver();

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
   * Serialise maintenance jobs with a Postgres advisory lock keyed by db.table.
   * The lock is bound to this JDBC connection and is released automatically if
   * the Spark driver exits.
   */
  void withMaintenanceLock(CheckedRunnable body) throws Exception {
    loadPostgresDriver();
    try (Connection c = DriverManager.getConnection(catalogUri, jdbcCredentials())) {
      try (PreparedStatement lock = c.prepareStatement("SELECT pg_advisory_lock(hashtext(?)::bigint)")) {
        lock.setString(1, tableName());
        lock.execute();
      }

      try {
        body.run();
      } finally {
        try (PreparedStatement unlock =
            c.prepareStatement("SELECT pg_advisory_unlock(hashtext(?)::bigint)")) {
          unlock.setString(1, tableName());
          unlock.execute();
        }
      }
    }
  }

  private Properties jdbcCredentials() {
    Properties creds = new Properties();
    creds.setProperty("user", jdbcUser);
    creds.setProperty("password", jdbcPassword);
    return creds;
  }

  private static void loadPostgresDriver() {
    try {
      Class.forName("org.postgresql.Driver");
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("PostgreSQL JDBC driver not on classpath", e);
    }
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
