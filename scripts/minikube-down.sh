#!/usr/bin/env bash
# Tear the demo down. By default deletes the whole minikube profile (the
# analog of `docker compose down -v` — frees all memory/disk). Pass
# --keep-cluster to only remove the app + operator and leave minikube running.
set -euo pipefail

NS=s3-table-dump
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

if [[ "${1:-}" == "--keep-cluster" ]]; then
  echo "==> deleting app + infra (keeping minikube)"
  kubectl delete -f k8s/maintenance-cron.yaml --ignore-not-found
  kubectl -n "${NS}" delete rolebinding flink-role-binding --ignore-not-found
  kubectl -n "${NS}" delete role flink --ignore-not-found
  kubectl -n "${NS}" delete serviceaccount flink --ignore-not-found
  # Ingest now runs on Kafka Connect; everything else lives in the namespace
  # and is removed by the namespace delete below.
  kubectl delete namespace "${NS}" --ignore-not-found
else
  echo "==> deleting the minikube profile entirely"
  minikube delete
fi
echo "done"
