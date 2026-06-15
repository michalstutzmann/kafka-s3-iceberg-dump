package com.example.flinkiceberg;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.IcebergSink;

/**
 * Ingest job: reads JSON events from a Kafka topic and continuously appends
 * them to an Iceberg table.
 *
 * <p>Iceberg <b>table maintenance</b> (compaction + snapshot expiration) runs
 * in a separate, independently-deployed job — see {@link IcebergMaintenanceJob}.
 * Splitting them gives ingest and maintenance independent lifecycles, failure
 * domains and tuning; shared catalog/lock wiring lives in {@link IcebergCatalog}.
 */
public final class KafkaToIcebergJob {

  private KafkaToIcebergJob() {}

  public static void main(String[] args) throws Exception {
    final String kafkaBootstrap = IcebergCatalog.env("KAFKA_BOOTSTRAP", "kafka:9092");
    final String kafkaTopic = IcebergCatalog.env("KAFKA_TOPIC", "events");

    IcebergCatalog catalog = IcebergCatalog.fromEnv();
    catalog.awaitJdbc(30, Duration.ofSeconds(2));
    catalog.ensureTable();
    TableLoader tableLoader = catalog.tableLoader();

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

    RowType rowType = FlinkSchemaUtil.convert(IcebergCatalog.SCHEMA);

    DataStream<RowData> rows =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source")
            .map(new JsonToRowData())
            .name("json-to-rowdata")
            .returns(InternalTypeInfo.of(rowType));

    IcebergSink.forRowData(rows)
        .tableLoader(tableLoader)
        .writeParallelism(1)
        .append();

    env.execute("kafka-to-iceberg");
  }
}
