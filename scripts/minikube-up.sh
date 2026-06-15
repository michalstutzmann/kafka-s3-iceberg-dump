#!/usr/bin/env bash
# Bring up the whole demo on minikube: build the app image into minikube's
# docker, deploy infra (Kafka, MinIO, Postgres, datagen), register the Kafka
# Connect ingest sink, and install the Spark maintenance CronJobs. Idempotent
# — safe to re-run.
#
#   scripts/minikube-up.sh
#
# Overridable via env: MINIKUBE_MEMORY, MINIKUBE_CPUS, IMAGE, CONNECT_IMAGE,
# APP_VERSION.
set -euo pipefail

NS=s3-table-dump
IMAGE="${IMAGE:-s3-table-dump:dev}"
# Ingest runs on Kafka Connect (Iceberg sink), built from Dockerfile.connect.
CONNECT_IMAGE="${CONNECT_IMAGE:-s3-table-dump-connect:dev}"
# 7600 fits an 8 GB Docker allocation (minikube refuses if the request
# exceeds Docker's available memory). Steady state only runs the ingest
# worker + infra; Spark maintenance pods run briefly on cron.
MINIKUBE_MEMORY="${MINIKUBE_MEMORY:-7600}"
MINIKUBE_CPUS="${MINIKUBE_CPUS:-4}"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

# git-semver-release is the version source of truth (same as the old builder);
# fall back to the Maven CI-friendly default for plain checkouts.
APP_VERSION="${APP_VERSION:-$(git-semver-release version 2>/dev/null || echo 0.0.0-SNAPSHOT)}"

echo "==> minikube (memory=${MINIKUBE_MEMORY}MB cpus=${MINIKUBE_CPUS})"
if ! minikube status >/dev/null 2>&1; then
  minikube start --driver=docker \
    --memory="${MINIKUBE_MEMORY}" --cpus="${MINIKUBE_CPUS}" --disk-size=20g
else
  echo "    minikube already running"
fi

echo "==> build images into minikube's docker (APP_VERSION=${APP_VERSION})"
# Build directly inside minikube's docker daemon so no registry is needed;
# imagePullPolicy: Never on the maintenance CronJobs / Connect pod then uses
# these local images. Two images: the Spark maintenance image, and the Kafka
# Connect worker image (Iceberg sink + JSON Schema converter) for ingest.
eval "$(minikube docker-env)"
docker build -t "${IMAGE}" --build-arg APP_VERSION="${APP_VERSION}" .
docker build -t "${CONNECT_IMAGE}" -f Dockerfile.connect .
eval "$(minikube docker-env -u)"

echo "==> namespace"
kubectl apply -f k8s/namespace.yaml

echo "==> remove legacy Flink maintenance resources if present"
kubectl -n "${NS}" delete flinkdeployment maintenance orphan-gc --ignore-not-found 2>/dev/null || true
helm uninstall flink-kubernetes-operator -n "${NS}" --ignore-not-found 2>/dev/null || true
kubectl -n "${NS}" delete rolebinding flink-role-binding --ignore-not-found
kubectl -n "${NS}" delete role flink --ignore-not-found
kubectl -n "${NS}" delete serviceaccount flink --ignore-not-found

echo "==> datagen ConfigMap (from scripts/datagen.sh — single source of truth)"
kubectl -n "${NS}" create configmap datagen-script \
  --from-file=datagen.sh=scripts/datagen.sh \
  --dry-run=client -o yaml | kubectl apply -f -

echo "==> infra (postgres, minio, kafka)"
kubectl apply -f k8s/postgres.yaml -f k8s/minio.yaml -f k8s/kafka.yaml
for d in postgres minio kafka; do
  kubectl -n "${NS}" rollout status deploy/"$d" --timeout=240s
done

echo "==> ingest: schema registry + kafka connect (Iceberg sink)"
kubectl apply -f k8s/schema-registry.yaml -f k8s/kafka-connect.yaml
kubectl -n "${NS}" set image deployment/kafka-connect kafka-connect="${CONNECT_IMAGE}"
kubectl -n "${NS}" rollout restart deployment/kafka-connect
for d in schema-registry kafka-connect; do
  kubectl -n "${NS}" rollout status deploy/"$d" --timeout=300s
done

echo "==> register the Iceberg sink connector"
# The Job's pod waits for Connect/SR and PUTs the connector config (idempotent).
# Jobs are largely immutable, so recreate on re-run to pick up config changes.
kubectl -n "${NS}" delete job connector-setup --ignore-not-found
kubectl apply -f k8s/connector-setup.yaml
kubectl -n "${NS}" wait --for=condition=complete job/connector-setup --timeout=180s

echo "==> datagen (JSON Schema producer, phased v1 -> v2 evolution)"
kubectl apply -f k8s/datagen.yaml

echo "==> Spark maintenance CronJobs"
kubectl apply -f k8s/maintenance-cron.yaml

cat <<EOF

Deployed. Watch it come up:
  kubectl -n ${NS} get pods,cronjob
  kubectl -n ${NS} logs deploy/datagen -f      # watch v1 -> v2 schema evolution

Ingest runs continuously on Kafka Connect (the Iceberg sink connector):
  kubectl -n ${NS} port-forward svc/kafka-connect 8083:8083
  curl -s localhost:8083/connectors/iceberg-sink/status | jq      # RUNNING?
  curl -s localhost:8081/subjects                                 # registered schemas (via SR port-forward below)

The maintenance + orphan-gc Spark jobs run as short-lived CronJob pods.
Cron schedules live in k8s/maintenance-cron.yaml.

  kubectl -n ${NS} get cronjob
  kubectl -n ${NS} get jobs   # one Job per cron firing
  # Force a maintenance run now:
  kubectl -n ${NS} create job --from=cronjob/maintenance-runner maintenance-now

Schema Registry / MinIO console:
  kubectl -n ${NS} port-forward svc/schema-registry 8081:8081    # JSON Schemas
  kubectl -n ${NS} port-forward svc/minio 9001:9001              # admin/password
EOF
