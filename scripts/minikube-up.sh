#!/usr/bin/env bash
# Bring up the whole demo on minikube: build the app image into minikube's
# docker, install the Flink Kubernetes Operator, deploy infra (Kafka, MinIO,
# Postgres, datagen) and the two Application-Mode Flink clusters (ingest +
# maintenance). Idempotent — safe to re-run.
#
#   scripts/minikube-up.sh
#
# Overridable via env: MINIKUBE_MEMORY, MINIKUBE_CPUS, OPERATOR_VERSION,
# IMAGE, APP_VERSION.
set -euo pipefail

NS=s3-table-dump
IMAGE="${IMAGE:-s3-table-dump:dev}"
OPERATOR_VERSION="${OPERATOR_VERSION:-1.14.0}"
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

echo "==> build ${IMAGE} into minikube's docker (APP_VERSION=${APP_VERSION})"
# Build directly inside minikube's docker daemon so no registry is needed;
# imagePullPolicy: Never on the FlinkDeployments then uses this local image.
eval "$(minikube docker-env)"
docker build -t "${IMAGE}" --build-arg APP_VERSION="${APP_VERSION}" .
eval "$(minikube docker-env -u)"

echo "==> namespace"
kubectl apply -f k8s/namespace.yaml

echo "==> Flink Kubernetes Operator ${OPERATOR_VERSION}"
helm repo add flink-operator \
  "https://downloads.apache.org/flink/flink-kubernetes-operator-${OPERATOR_VERSION}/" >/dev/null 2>&1 || true
helm repo update flink-operator >/dev/null
# webhook.create=false avoids the cert-manager dependency (the webhook only
# does validation/defaulting); rbac.create + watchNamespaces makes the chart
# create the `flink` ServiceAccount + RBAC the JobManagers need in $NS.
helm upgrade --install flink-kubernetes-operator \
  flink-operator/flink-kubernetes-operator \
  --version "${OPERATOR_VERSION}" \
  --namespace "${NS}" \
  --set watchNamespaces="{${NS}}" \
  --set webhook.create=false
kubectl -n "${NS}" rollout status deploy/flink-kubernetes-operator --timeout=180s

echo "==> datagen ConfigMap (from scripts/datagen.sh — single source of truth)"
kubectl -n "${NS}" create configmap datagen-script \
  --from-file=datagen.sh=scripts/datagen.sh \
  --dry-run=client -o yaml | kubectl apply -f -

echo "==> infra (postgres, minio, kafka, datagen)"
kubectl apply -f k8s/postgres.yaml -f k8s/minio.yaml -f k8s/kafka.yaml -f k8s/datagen.yaml
for d in postgres minio kafka; do
  kubectl -n "${NS}" rollout status deploy/"$d" --timeout=240s
done

echo "==> Flink Application clusters (ingest + maintenance)"
kubectl apply -f k8s/flink-ingest.yaml -f k8s/flink-maintenance.yaml

cat <<EOF

Deployed. Watch it come up:
  kubectl -n ${NS} get flinkdeployment,pods
  kubectl -n ${NS} logs deploy/datagen -f

Flink UIs (separate cluster per job — the whole point):
  kubectl -n ${NS} port-forward svc/ingest-rest 8081:8081        # ingest
  kubectl -n ${NS} port-forward svc/maintenance-rest 8082:8081   # maintenance
MinIO console:
  kubectl -n ${NS} port-forward svc/minio 9001:9001              # admin/password
EOF
