package com.example.flinkiceberg;

import java.time.Instant;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

/**
 * Parses a JSON event into an Iceberg-compatible {@link RowData}.
 *
 * <p>Expected payload:
 * <pre>{@code
 * {"id":"evt-1","event_type":"click","user_id":"user-2","amount":42.50,
 *  "event_time":"2026-05-18T10:00:00Z"}
 * }</pre>
 *
 * Field order must match {@code KafkaToIcebergJob.SCHEMA}:
 * id, event_type, user_id, amount, event_time.
 */
public final class JsonToRowData implements MapFunction<String, RowData> {

  private static final long serialVersionUID = 1L;

  // ObjectMapper is not guaranteed serializable across the Flink job graph;
  // build it lazily on each task instance.
  private transient ObjectMapper mapper;

  @Override
  public RowData map(String json) throws Exception {
    if (mapper == null) {
      mapper = new ObjectMapper();
    }
    JsonNode node = mapper.readTree(json);

    GenericRowData row = new GenericRowData(5);
    row.setField(0, StringData.fromString(text(node, "id")));
    row.setField(1, StringData.fromString(text(node, "event_type")));
    row.setField(2, StringData.fromString(text(node, "user_id")));
    row.setField(3, node.hasNonNull("amount") ? node.get("amount").asDouble() : null);

    String ts = text(node, "event_time");
    row.setField(
        4, ts == null ? null : TimestampData.fromInstant(Instant.parse(ts)));
    return row;
  }

  private static String text(JsonNode node, String field) {
    JsonNode v = node.get(field);
    return (v == null || v.isNull()) ? null : v.asText();
  }
}
