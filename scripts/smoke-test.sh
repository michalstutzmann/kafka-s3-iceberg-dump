#!/usr/bin/env bash
# End-to-end smoke test for the Kafka -> Polaris (REST) -> Iceberg/MinIO demo.
#
# Verifies the whole pipeline after `scripts/minikube-up.sh`:
#   1. infra + Polaris + ingest + datagen pods are Ready
#   2. the bootstrap/setup Jobs completed
#   3. Polaris serves the `iceberg` catalog with namespace `db` (REST API)
#   4. the Kafka Connect sink is RUNNING
#   5. rows are actually landing in db.events (Iceberg snapshot total-records > 0)
#   6. both Spark maintenance jobs run green and use the Kubernetes Lease
#
# Usage:
#   scripts/smoke-test.sh            # assume the stack is already up
#   scripts/smoke-test.sh --up       # run scripts/minikube-up.sh first
#   SKIP_MAINTENANCE=1 scripts/smoke-test.sh   # skip the slow maintenance runs
#
# Exit code is non-zero if any check fails. Safe to re-run.
set -uo pipefail

NS=s3-table-dump
REALM=POLARIS
CLIENT=root:s3cr3t
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

PASS=0
FAIL=0
PF_PIDS=()
TMP_JOBS=()

green() { printf '\033[32m%s\033[0m\n' "$*"; }
red()   { printf '\033[31m%s\033[0m\n' "$*"; }
info()  { printf '\033[36m==>\033[0m %s\n' "$*"; }
pass()  { green "  PASS: $*"; PASS=$((PASS + 1)); }
fail()  { red   "  FAIL: $*"; FAIL=$((FAIL + 1)); }

cleanup() {
  for pid in "${PF_PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  for j in "${TMP_JOBS[@]:-}"; do kubectl -n "$NS" delete job "$j" --ignore-not-found >/dev/null 2>&1 || true; done
}
trap cleanup EXIT

# Retry a command until it succeeds or the timeout (seconds) elapses.
# retry <timeout> <sleep> <description> -- <cmd...>
retry() {
  local timeout=$1 step=$2 desc=$3; shift 3; [ "$1" = "--" ] && shift
  local deadline=$((SECONDS + timeout))
  until "$@"; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      return 1
    fi
    sleep "$step"
  done
}

# Start a background port-forward (callers retry their real request, so we just
# give it a moment to establish).
port_forward() {
  local svc=$1 remote=$2 local_port=$3
  kubectl -n "$NS" port-forward "svc/$svc" "$local_port:$remote" >/dev/null 2>&1 &
  PF_PIDS+=("$!")
  sleep 2
}

polaris_token() {
  curl -s "http://127.0.0.1:8181/api/catalog/v1/oauth/tokens" \
    --user "$CLIENT" -H "Polaris-Realm: $REALM" \
    -d grant_type=client_credentials -d scope=PRINCIPAL_ROLE:ALL | jq -r '.access_token // empty'
}

# Predicates used with retry (run in the current shell, so functions/globals work).
fetch_token()        { TOKEN=$(polaris_token); [ -n "$TOKEN" ]; }
cat_get()            { curl -s -H "Authorization: Bearer $TOKEN" -H "Polaris-Realm: $REALM" "http://127.0.0.1:8181$1"; }
connector_running()  { curl -s http://127.0.0.1:8083/connectors/iceberg-sink/status \
                         | jq -e '.connector.state=="RUNNING" and ([.tasks[].state] | all(.=="RUNNING"))' >/dev/null 2>&1; }
records() {
  cat_get /api/catalog/v1/iceberg/namespaces/db/tables/events \
    | jq -r 'if .metadata then (.metadata."current-snapshot-id" as $c
              | (.metadata.snapshots // []) | map(select(.["snapshot-id"]==$c)) | .[0].summary["total-records"] // "0")
             else "" end' 2>/dev/null
}
has_records() { local r; r=$(records); [ -n "$r" ] && [ "$r" != "0" ]; }

# ---------------------------------------------------------------------------
[ "${1:-}" = "--up" ] && { info "Bringing the stack up (scripts/minikube-up.sh)"; scripts/minikube-up.sh; }

for bin in kubectl jq curl; do
  command -v "$bin" >/dev/null || { red "missing prerequisite: $bin"; exit 2; }
done
kubectl get ns "$NS" >/dev/null 2>&1 || { red "namespace $NS not found — run scripts/minikube-up.sh (or pass --up)"; exit 2; }

# ---- 1. Deployments Ready ----
info "1. Deployments are Ready"
for d in postgres minio kafka polaris schema-registry kafka-connect datagen; do
  if kubectl -n "$NS" rollout status "deploy/$d" --timeout=180s >/dev/null 2>&1; then
    pass "deployment/$d ready"
  else
    fail "deployment/$d not ready"
  fi
done

# ---- 2. Bootstrap/setup Jobs completed ----
info "2. Setup Jobs completed"
for j in polaris-setup connector-setup; do
  if kubectl -n "$NS" wait --for=condition=complete "job/$j" --timeout=180s >/dev/null 2>&1; then
    pass "job/$j complete"
  else
    fail "job/$j did not complete"; kubectl -n "$NS" logs "job/$j" --tail=20 2>/dev/null | sed 's/^/      /'
  fi
done

# ---- 3. Polaris serves the iceberg catalog ----
info "3. Polaris REST catalog"
port_forward polaris 8181 8181
TOKEN=""
if retry 30 2 "obtain token" -- fetch_token; then
  pass "obtained root OAuth2 token"
else
  fail "could not obtain Polaris token"
fi

if [ -n "$TOKEN" ]; then
  if cat_get /api/management/v1/catalogs/iceberg | jq -e '.name == "iceberg"' >/dev/null 2>&1; then
    pass "catalog 'iceberg' exists"
  else
    fail "catalog 'iceberg' missing"
  fi
  if cat_get /api/catalog/v1/iceberg/namespaces | jq -e '.namespaces[] | select(.[0]=="db")' >/dev/null 2>&1; then
    pass "namespace 'db' exists"
  else
    fail "namespace 'db' missing"
  fi
fi

# ---- 4. Kafka Connect sink RUNNING ----
info "4. Kafka Connect sink"
port_forward kafka-connect 8083 8083
if retry 60 3 "connector RUNNING" -- connector_running; then
  pass "connector iceberg-sink + tasks RUNNING"
else
  fail "connector iceberg-sink not RUNNING"
  curl -s http://127.0.0.1:8083/connectors/iceberg-sink/status | jq . 2>/dev/null | sed 's/^/      /'
fi

# ---- 5. Rows landing in db.events ----
info "5. Data flowing into db.events (Iceberg snapshot total-records)"
# Table is created on first commit; datagen + 60s commit interval means up to ~2 min.
if retry 180 5 "table + rows" -- has_records; then
  pass "db.events has $(records) records"
else
  n=$(records)
  if [ -n "$n" ]; then
    fail "db.events present but total-records=$n after 180s"
  else
    fail "db.events table not found after 180s (ingest not committing?)"
  fi
fi

# ---- 6. Spark maintenance jobs + Kubernetes Lease ----
if [ "${SKIP_MAINTENANCE:-0}" = "1" ]; then
  info "6. Maintenance runs SKIPPED (SKIP_MAINTENANCE=1)"
else
  info "6. Spark maintenance jobs (REST catalog + Kubernetes Lease)"
  # Capture logs fully, then grep from a here-string. Piping `kubectl logs` into
  # `grep -q` would make grep close the pipe on first match while kubectl is
  # still streaming, and under `pipefail` kubectl's SIGPIPE fails the pipeline.
  log_has() {
    local out
    out=$(kubectl -n "$NS" logs "job/$1" --tail=-1 --all-containers 2>/dev/null) || return 1
    grep -qF "$2" <<<"$out"
  }
  run_job() { # run_job <cronjob> <jobname> <expected-log-substring>
    local cron=$1 name=$2 expect=$3
    kubectl -n "$NS" delete job "$name" --ignore-not-found >/dev/null 2>&1
    kubectl -n "$NS" create job --from="cronjob/$cron" "$name" >/dev/null
    TMP_JOBS+=("$name")
    if kubectl -n "$NS" wait --for=condition=complete "job/$name" --timeout=300s >/dev/null 2>&1; then
      # Driver stdout can lag the Complete condition by a moment; retry the grep.
      if retry 30 3 "logs for $name" -- log_has "$name" "$expect"; then
        pass "$cron ran green ('$expect' seen)"
      else
        fail "$cron completed but '$expect' not in logs"
      fi
    else
      fail "$cron job did not complete"
      kubectl -n "$NS" logs "job/$name" --tail=25 2>/dev/null | sed 's/^/      /'
    fi
  }
  run_job maintenance-runner maintenance-smoke "RewriteDataFiles result"
  run_job orphan-gc-runner    orphan-gc-smoke   "DeleteOrphanFiles result"

  if kubectl -n "$NS" get lease iceberg-maintenance >/dev/null 2>&1; then
    pass "Lease iceberg-maintenance exists (mutex was used)"
  else
    fail "Lease iceberg-maintenance not found (lock never acquired?)"
  fi
fi

# ---- summary ----
echo
info "Summary: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ] && { green "SMOKE TEST PASSED"; exit 0; } || { red "SMOKE TEST FAILED"; exit 1; }
