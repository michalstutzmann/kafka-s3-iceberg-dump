#!/bin/sh
# Continuously publishes JSON events to the Kafka topic via kcat.
set -eu

BROKER="${BROKER:-kafka:9092}"
TOPIC="${TOPIC:-events}"
INTERVAL="${INTERVAL:-0.5}"

echo "datagen -> broker=$BROKER topic=$TOPIC interval=${INTERVAL}s"

i=0
while true; do
  i=$((i + 1))
  type=$(awk 'BEGIN{srand();a[0]="click";a[1]="view";a[2]="purchase";print a[int(rand()*3)]}')
  amount=$(awk 'BEGIN{srand();printf "%.2f", rand()*100}')
  ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  echo "{\"id\":\"evt-$i\",\"event_type\":\"$type\",\"user_id\":\"user-$((i % 5))\",\"amount\":$amount,\"event_time\":\"$ts\"}"
  sleep "$INTERVAL"
done | kcat -b "$BROKER" -t "$TOPIC" -P
