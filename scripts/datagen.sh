#!/usr/bin/env bash
# Publishes JSON-Schema-serialized events to Kafka via Confluent's
# kafka-json-schema-console-producer (so the schema is registered in Schema
# Registry and records carry the magic-byte + schema-id wire format the
# Iceberg sink connector's JsonSchemaConverter expects).
#
# It runs in TWO PHASES to demonstrate schema evolution end to end:
#   v1  : id, event_type, user_id, amount, event_time           (PHASE1_COUNT events)
#   v2  : ... + an OPTIONAL "currency" field                     (forever)
# Registering v2 adds a new optional property — a backward-compatible change —
# so Schema Registry accepts it as version 2, and the connector
# (evolve-schema-enabled) ALTERs the Iceberg table to add the `currency`
# column live, with no redeploy.
#
# event_time is declared as the Kafka Connect Timestamp logical type
# ({"type":"number", title org.apache.kafka.connect.data.Timestamp,
# connect.type int64) so the connector maps it to an Iceberg `timestamptz`
# column — its on-the-wire value is therefore epoch MILLISECONDS, a number.
#
# bash (not POSIX sh) for /dev/tcp readiness checks; the cp-schema-registry
# image provides bash + the producer. No $RANDOM: a tiny LCG gives determinism.
set -eu

BROKER="${BROKER:-kafka:9092}"
TOPIC="${TOPIC:-events}"
SR_URL="${SR_URL:-http://schema-registry:8081}"
INTERVAL="${INTERVAL:-0.5}"
PHASE1_COUNT="${PHASE1_COUNT:-60}"   # ~30s of v1 at INTERVAL=0.5 before evolving
PRODUCER="${PRODUCER:-kafka-json-schema-console-producer}"

SR_HOST="${SR_URL#http://}"; SR_HOST="${SR_HOST%%:*}"
SR_PORT="${SR_URL##*:}"
BROKER_HOST="${BROKER%%:*}"; BROKER_PORT="${BROKER##*:}"

wait_tcp() {  # host port label
  echo "waiting for $3 ($1:$2)..."
  until (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null; do sleep 3; done
  exec 3>&- 2>/dev/null || true
  echo "$3 reachable"
}
wait_tcp "$BROKER_HOST" "$BROKER_PORT" kafka
wait_tcp "$SR_HOST" "$SR_PORT" schema-registry

# JSON Schemas (single-line). Both use a CLOSED content model
# ("additionalProperties":false): under Confluent's JSON Schema compatibility
# rules, adding a property to an OPEN model is NOT backward-compatible
# (PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL — old data could already carry a
# differently-typed "currency"), whereas adding an optional property to a
# CLOSED model IS backward-compatible. So v2 (adds optional "currency") is
# accepted as version 2 only because the model is closed.
SCHEMA_V1='{"type":"object","title":"Event","additionalProperties":false,"properties":{"id":{"type":"string"},"event_type":{"type":"string"},"user_id":{"type":"string"},"amount":{"type":"number"},"event_time":{"type":"number","title":"org.apache.kafka.connect.data.Timestamp","connect.type":"int64"}},"required":["id"]}'
SCHEMA_V2='{"type":"object","title":"Event","additionalProperties":false,"properties":{"id":{"type":"string"},"event_type":{"type":"string"},"user_id":{"type":"string"},"amount":{"type":"number"},"event_time":{"type":"number","title":"org.apache.kafka.connect.data.Timestamp","connect.type":"int64"},"currency":{"type":"string"}},"required":["id"]}'

i=0
seed=$$
emit() {  # $1 = with_currency (1/0)
  i=$((i + 1))
  seed=$(( (seed * 1103515245 + 12345) & 2147483647 ))
  case $(( seed % 3 )) in
    0) type=click ;;
    1) type=view ;;
    *) type=purchase ;;
  esac
  seed=$(( (seed * 1103515245 + 12345) & 2147483647 ))
  cents=$(( seed % 10000 ))
  amount="$(( cents / 100 )).$(printf '%02d' $(( cents % 100 )))"
  ms=$(( $(date -u +%s) * 1000 ))
  if [ "$1" = "1" ]; then
    seed=$(( (seed * 1103515245 + 12345) & 2147483647 ))
    case $(( seed % 3 )) in 0) cur=USD ;; 1) cur=EUR ;; *) cur=GBP ;; esac
    printf '{"id":"evt-%d","event_type":"%s","user_id":"user-%d","amount":%s,"event_time":%d,"currency":"%s"}\n' \
      "$i" "$type" "$((i % 5))" "$amount" "$ms" "$cur"
  else
    printf '{"id":"evt-%d","event_type":"%s","user_id":"user-%d","amount":%s,"event_time":%d}\n' \
      "$i" "$type" "$((i % 5))" "$amount" "$ms"
  fi
}

produce() {  # $1 = schema, then reads events from stdin
  "$PRODUCER" --bootstrap-server "$BROKER" --topic "$TOPIC" \
    --property schema.registry.url="$SR_URL" \
    --property value.schema="$1"
}

echo "datagen v1 -> $PHASE1_COUNT events (no currency), then v2 forever (with currency)"

# Phase 1: v1 schema. Emit a fixed number of lines, then EOF closes the
# producer so phase 2 can register v2.
{ n=0; while [ "$n" -lt "$PHASE1_COUNT" ]; do emit 0; n=$((n + 1)); sleep "$INTERVAL"; done; } | produce "$SCHEMA_V1"

echo "==> evolving schema: registering v2 (adds optional currency)"

# Phase 2: v2 schema, forever.
{ while true; do emit 1; sleep "$INTERVAL"; done; } | produce "$SCHEMA_V2"
