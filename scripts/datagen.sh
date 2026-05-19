#!/bin/sh
# Continuously publishes JSON events to the Kafka topic. Runs in the
# apache/kafka image (multi-arch) and pipes into kafka-console-producer, so
# no extra producer image is needed. POSIX sh only (no kcat/awk/$RANDOM):
# a tiny LCG provides the pseudo-randomness.
set -eu

BROKER="${BROKER:-kafka:9092}"
TOPIC="${TOPIC:-events}"
INTERVAL="${INTERVAL:-0.5}"
PRODUCER="${PRODUCER:-/opt/kafka/bin/kafka-console-producer.sh}"

echo "datagen -> broker=$BROKER topic=$TOPIC interval=${INTERVAL}s"

i=0
seed=$$
gen() {
  while true; do
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
    ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    echo "{\"id\":\"evt-$i\",\"event_type\":\"$type\",\"user_id\":\"user-$((i % 5))\",\"amount\":$amount,\"event_time\":\"$ts\"}"
    sleep "$INTERVAL"
  done
}

gen | "$PRODUCER" --bootstrap-server "$BROKER" --topic "$TOPIC"
