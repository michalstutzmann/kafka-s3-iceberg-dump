#!/usr/bin/env bash
# Tear the demo down. By default deletes the whole minikube profile (the
# analog of `docker compose down -v` — frees all memory/disk). Pass
# --keep-cluster to only remove the app + operator and leave minikube running.
set -euo pipefail

NS=s3-table-dump
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

if [[ "${1:-}" == "--keep-cluster" ]]; then
  echo "==> deleting Flink apps + infra (keeping minikube)"
  kubectl delete -f k8s/flink-ingest.yaml -f k8s/flink-maintenance.yaml --ignore-not-found
  helm uninstall flink-kubernetes-operator -n "${NS}" --ignore-not-found 2>/dev/null || true
  kubectl delete namespace "${NS}" --ignore-not-found
else
  echo "==> deleting the minikube profile entirely"
  minikube delete
fi
echo "done"
