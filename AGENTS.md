# AGENTS.md

Guidance for AI agents working in this repository.

## What This Is

**Kafka S3 Iceberg Dump** is a minikube demo that streams JSON events from Kafka
into an Apache Iceberg table on MinIO, then runs Iceberg table maintenance as
scheduled Spark batch jobs.

Two runtimes are deliberately split:

* **Ingest = Apache Iceberg Kafka Connect sink connector.** A Kafka Connect
  worker runs `org.apache.iceberg.connect.IcebergSinkConnector`, registered by
  `k8s/connector-setup.yaml`. Records are produced as JSON Schema through
  Confluent Schema Registry. The connector's `JsonSchemaConverter` maps the
  registry schema to Iceberg types and owns the table:
  `auto-create-enabled` + `evolve-schema-enabled` create and evolve `db.events`.
  Do not reintroduce a hardcoded Java schema or mapper.
* **Maintenance = two Spark CronJobs.** `maintenance-runner` launches
  `com.example.sparkiceberg.IcebergMaintenanceJob` for `RewriteDataFiles` and
  `ExpireSnapshots`; `orphan-gc-runner` launches
  `com.example.sparkiceberg.IcebergOrphanGcJob` for `DeleteOrphanFiles`. Both
  run in the same Spark image, as separate Kubernetes Jobs with independent
  pod resources and failure domains.

The Iceberg catalog is **Apache Polaris**, reached over the Iceberg REST
protocol (`k8s/polaris.yaml`). Ingest, maintenance, and orphan GC all share the
one Polaris catalog (`iceberg`) and one warehouse on MinIO. Polaris persists its
metadata in Postgres (`relational-jdbc`) and owns the MinIO credentials, which
it *vends* to clients — so neither Kafka Connect nor Spark carries S3 keys, and
neither talks to Postgres directly. The two Spark maintenance jobs serialize on
a Kubernetes `Lease` (`iceberg-maintenance`), not a database lock.

## Build And Run

```bash
./mvnw -DskipTests package
scripts/minikube-up.sh
scripts/minikube-down.sh
scripts/minikube-down.sh --keep-cluster
```

`scripts/minikube-up.sh` builds two local minikube images:

* `Dockerfile` -> `s3-table-dump:dev`: Maven build stage, then
  `apache/spark:4.0.1-scala2.13-java21-ubuntu` with the maintenance jar at
  `/opt/spark/work-dir/app.jar`.
* `Dockerfile.connect` -> `s3-table-dump-connect:dev`: Confluent Kafka Connect
  with the Iceberg connector plugin, Iceberg AWS/Hadoop dependencies, and the
  Confluent JSON Schema converter.

The Polaris server and admin tool run from the upstream `apache/polaris:1.5.0`
and `apache/polaris-admin-tool:1.5.0` images, pulled from Docker Hub (not built
locally).

No local JDK/Maven is required for the minikube path. The locally built images
are built inside minikube's Docker daemon and consumed with
`imagePullPolicy: Never`.

## Layout

| Path | Purpose |
|---|---|
| `src/main/java/.../IcebergCatalog.java` | Shared Spark maintenance REST catalog config, Spark session config, Polaris readiness preflight, and the maintenance lock entry point |
| `src/main/java/.../KubernetesLease.java` | Kubernetes `Lease`-based maintenance lock (replaces the Postgres advisory lock) |
| `src/main/java/.../IcebergMaintenanceJob.java` | Spark rewrite + expire job |
| `src/main/java/.../IcebergOrphanGcJob.java` | Spark orphan-file cleanup job |
| `pom.xml` | Spark maintenance jar dependencies and shade config |
| `kafka-connect/pom.xml` | Build-only Kafka Connect plugin dependency collector |
| `Dockerfile` | Spark maintenance image |
| `Dockerfile.connect` | Kafka Connect ingest image |
| `k8s/polaris.yaml` | Polaris REST catalog server (+ admin-tool bootstrap initContainer) |
| `k8s/polaris-setup.yaml` | One-shot Job: create the `iceberg` catalog, grant content access, make the `db` namespace |
| `k8s/maintenance-cron.yaml` | Two Kubernetes CronJobs for Spark maintenance |
| `k8s/maintenance-rbac.yaml` | ServiceAccount + Role/RoleBinding for the maintenance Lease |
| `scripts/minikube-up.sh` / `scripts/minikube-down.sh` | Bring up / tear down minikube stack |
| `scripts/smoke-test.sh` | End-to-end verification (catalog, ingest, rows, maintenance, Lease) |
| `scripts/datagen.sh` | JSON Schema producer, v1 -> v2 schema evolution demo |

## Invariants

* **Ingest is Kafka Connect, not Spark.** Spark is only for maintenance. Do not
  add a Spark ingest job.
* **The connector owns table creation and schema evolution.** Keep
  `auto-create-enabled` and `evolve-schema-enabled`; do not add Java schema,
  partition spec, or JSON-to-row mapping code.
* **Keep Iceberg versions in lockstep.** Root `pom.xml` and
  `kafka-connect/pom.xml` must use the same `iceberg.version`. Current value:
  `1.10.1`.
* **Spark version is pinned.** Current value: Spark `4.0.1`, Scala `2.13`,
  Java `21`. Do not use floating image tags or Maven versions.
* **Polaris version is pinned.** `apache/polaris` and `apache/polaris-admin-tool`
  are both `1.5.0`. The bootstrap initContainer image and the server image must
  match. Do not use `latest`.
* **One Polaris catalog, two writers.** Kafka Connect and Spark must describe the
  same REST catalog: `type=rest`, uri `http://polaris:8181/api/catalog`,
  warehouse/catalog name `iceberg`, credential `root:s3cr3t`, scope
  `PRINCIPAL_ROLE:ALL`, and table `db.events`. Do not reintroduce a JDBC catalog
  (`catalog-impl`, `jdbc.*`) or hardcode S3 credentials on the clients.
* **Polaris owns MinIO credentials; clients use vended credentials.** The MinIO
  keys live only on the Polaris server (`AWS_*` in `k8s/polaris.yaml`) and in the
  catalog storage config. Clients send the
  `header.X-Iceberg-Access-Delegation=vended-credentials` delegation header and
  get S3 access (including the endpoint) from Polaris. Do not add `s3.*` keys to
  the connector or `IcebergCatalog`.
* **Polaris persists to the shared Postgres.** `POLARIS_PERSISTENCE_TYPE` is
  `relational-jdbc` against `jdbc:postgresql://postgres:5432/iceberg`. The
  admin-tool `bootstrap` (initContainer) seeds the `POLARIS` realm before the
  server starts; it is idempotent. Postgres serves only Polaris now — no other
  component connects to it.
* **Maintenance jobs serialize on a Kubernetes Lease, not a database.** Both
  Spark jobs call `IcebergCatalog.withMaintenanceLock()`, backed by
  `KubernetesLease` on a single `coordination.k8s.io/v1` Lease
  (`iceberg-maintenance`). Keep the shared lease name; the CronJob pods need the
  `maintenance` ServiceAccount + Role from `k8s/maintenance-rbac.yaml`. Do not
  give orphan GC a different lease.
* **Orphan cleanup stays separate.** Keep `DeleteOrphanFiles` in its own
  CronJob so it has an isolated failure domain and can be scheduled less often.
* **Delete-orphan age must exceed ingest commit cadence.** The connector commit
  interval is `iceberg.control.commit.interval-ms=60000`; orphan cleanup uses a
  10 minute age threshold. Do not shrink it near the commit cadence.
* **Hadoop client jars stay bundled.** Iceberg catalog loading needs
  `org.apache.hadoop.conf.Configuration` even with `S3FileIO`.
* **No Postgres JDBC driver on the clients.** Neither the Spark maintenance jar
  nor the Kafka Connect plugin talks to Postgres — only Polaris does (with its
  own bundled driver). Do not re-add `org.postgresql:postgresql` to `pom.xml` or
  `kafka-connect/pom.xml`.
* **MinIO uses `alpine/minio`.** Keep the bucket setup sidecar; do not
  reintroduce `minio/mc` as a separate Job because the `emptyDir` data volume
  is pod-local.

## Schema Mapping Gotchas

* `event_time` is a Kafka Connect `Timestamp` logical type and must be epoch
  milliseconds on the wire.
* JSON Schema evolution is governed by Schema Registry compatibility. The demo
  relies on a closed content model (`additionalProperties: false`) and an
  optional v2 `currency` field.
* A required new field, incompatible type change, rename, or adding a field to
  an open content model will be rejected by Schema Registry and the table will
  not evolve.

## Verification

`scripts/smoke-test.sh` runs all of the below as one pass/fail check (use
`--up` to bring the stack up first, `SKIP_MAINTENANCE=1` to skip the Spark runs).

Useful commands:

```bash
kubectl -n s3-table-dump get pods,cronjob,jobs
kubectl -n s3-table-dump logs deploy/datagen -f
kubectl -n s3-table-dump logs deploy/polaris            # catalog server
kubectl -n s3-table-dump logs job/polaris-setup         # catalog/namespace creation
kubectl -n s3-table-dump get lease iceberg-maintenance  # maintenance lock
kubectl -n s3-table-dump port-forward svc/kafka-connect 8083:8083
curl -s localhost:8083/connectors/iceberg-sink/status | jq

# Query Polaris directly (port-forward svc/polaris 8181:8181 first):
TOKEN=$(curl -s localhost:8181/api/catalog/v1/oauth/tokens \
  --user root:s3cr3t -H 'Polaris-Realm: POLARIS' \
  -d grant_type=client_credentials -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)
curl -s -H "Authorization: Bearer $TOKEN" -H 'Polaris-Realm: POLARIS' \
  localhost:8181/api/catalog/v1/iceberg/namespaces/db/tables | jq

kubectl -n s3-table-dump create job --from=cronjob/maintenance-runner maintenance-now
kubectl -n s3-table-dump create job --from=cronjob/orphan-gc-runner orphan-gc-now
kubectl -n s3-table-dump logs -l job-name=maintenance-now --tail=-1
kubectl -n s3-table-dump logs -l job-name=orphan-gc-now --tail=-1
```

## Versioning

Releases use
[git-semver-release](https://github.com/michalstutzmann/git-semver-release)
with manual bumps (`patch`, `minor`, `major`). The Maven project version is
CI-friendly (`<version>${revision}</version>`). Do not hardcode a real version
in `pom.xml`; release tooling supplies `-Drevision`.
