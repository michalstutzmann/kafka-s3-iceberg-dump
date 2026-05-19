package com.example.flinkiceberg;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.hadoop.conf.Configuration;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.ExpireSnapshots;
import org.apache.iceberg.flink.maintenance.api.JdbcLockFactory;
import org.apache.iceberg.flink.maintenance.api.RewriteDataFiles;
import org.apache.iceberg.flink.maintenance.api.TableMaintenance;
import org.apache.iceberg.flink.maintenance.api.TriggerLockFactory;
import org.apache.iceberg.flink.sink.IcebergSink;
import org.apache.iceberg.types.Types;

/**
 * Reads JSON events from a Kafka topic and continuously appends them to an
 * Iceberg table, while running Iceberg table maintenance (data-file compaction
 * and snapshot expiration) in the same Flink job.
 *
 * <p>All knobs are environment-driven so the same jar runs locally and in the
 * Docker Compose stack.
 */
public final class KafkaToIcebergJob {

  // Iceberg table schema. Field order here is the order produced by the
  // JSON -> RowData mapper and expected by the Iceberg sink.
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.StringType.get()),
          Types.NestedField.optional(2, "event_type", Types.StringType.get()),
          Types.NestedField.optional(3, "user_id", Types.StringType.get()),
          Types.NestedField.optional(4, "amount", Types.DoubleType.get()),
          Types.NestedField.optional(5, "event_time", Types.TimestampType.withZone()));

  // Identity-partition by event_type so the demo produces many small data
  // files that RewriteDataFiles can later compact.
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("event_type").build();

  private KafkaToIcebergJob() {}

  public static void main(String[] args) throws Exception {
    final String kafkaBootstrap = env("KAFKA_BOOTSTRAP", "kafka:9092");
    final String kafkaTopic = env("KAFKA_TOPIC", "events");
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

    // ---- Iceberg JDBC catalog over MinIO (S3FileIO) -------------------------
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

    TableIdentifier tableId = TableIdentifier.of(dbName, tableName);

    // Create namespace + table up-front (runs in the Flink client JVM).
    // org.apache.iceberg.catalog.Catalog is not AutoCloseable; close manually.
    Catalog catalog = catalogLoader.loadCatalog();
    try {
      Namespace ns = Namespace.of(dbName);
      if (catalog instanceof SupportsNamespaces nsCatalog
          && !nsCatalog.namespaceExists(ns)) {
        nsCatalog.createNamespace(ns);
      }
      if (!catalog.tableExists(tableId)) {
        catalog.createTable(tableId, SCHEMA, SPEC, Map.of("format-version", "2"));
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

    TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableId);

    // ---- Flink job ----------------------------------------------------------
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // Iceberg commits on checkpoint; checkpointing must be enabled.
    env.enableCheckpointing(Duration.ofSeconds(30).toMillis());

    KafkaSource<String> source =
        KafkaSource.<String>builder()
            .setBootstrapServers(kafkaBootstrap)
            .setTopics(kafkaTopic)
            .setGroupId("flink-kafka-to-iceberg")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

    RowType rowType = FlinkSchemaUtil.convert(SCHEMA);

    DataStream<RowData> rows =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source")
            .map(new JsonToRowData())
            .name("json-to-rowdata")
            .returns(InternalTypeInfo.of(rowType));

    IcebergSink.forRowData(rows)
        .tableLoader(tableLoader)
        .writeParallelism(1)
        .append();

    // ---- Iceberg table maintenance (compaction + snapshot expiration) -------
    // Lock prevents two maintenance runs from colliding; here it shares the
    // catalog's Postgres. JdbcLockFactory auto-creates its lock table.
    TriggerLockFactory lockFactory =
        new JdbcLockFactory(
            catalogUri,
            dbName + "." + tableName,
            Map.of(
                "jdbc.user", jdbcUser,
                "jdbc.password", jdbcPassword,
                JdbcLockFactory.INIT_LOCK_TABLES_PROPERTY, "true"));

    TableMaintenance.forTable(env, tableLoader, lockFactory)
        .uidSuffix("iceberg-maintenance")
        .rateLimit(Duration.ofMinutes(1))
        .lockCheckDelay(Duration.ofSeconds(10))
        .add(
            RewriteDataFiles.builder()
                .scheduleOnCommitCount(3)
                .targetFileSizeBytes(64L * 1024 * 1024)
                .partialProgressEnabled(true)
                .partialProgressMaxCommits(2))
        .add(
            ExpireSnapshots.builder()
                .scheduleOnCommitCount(5)
                .maxSnapshotAge(Duration.ofMinutes(5))
                .retainLast(3)
                .deleteBatchSize(100))
        .append();

    env.execute("kafka-to-iceberg");
  }

  private static String env(String key, String defaultValue) {
    String v = System.getenv(key);
    return (v == null || v.isBlank()) ? defaultValue : v;
  }
}
