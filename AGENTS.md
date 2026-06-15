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

Ingest, maintenance, and orphan GC share one Iceberg JDBC catalog on Postgres
and one warehouse on MinIO. The two Spark maintenance jobs also share a
Postgres advisory lock keyed by `db.events`, so overlapping maintenance runs
serialize.

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
  with the Iceberg connector plugin, Iceberg AWS/Postgres/Hadoop dependencies,
  and the Confluent JSON Schema converter.

No local JDK/Maven is required for the minikube path. Images are built inside
minikube's Docker daemon and consumed with `imagePullPolicy: Never`.

## Layout

| Path | Purpose |
|---|---|
| `src/main/java/.../IcebergCatalog.java` | Shared Spark maintenance catalog config, Spark session config, JDBC preflight, and Postgres advisory lock |
| `src/main/java/.../IcebergMaintenanceJob.java` | Spark rewrite + expire job |
| `src/main/java/.../IcebergOrphanGcJob.java` | Spark orphan-file cleanup job |
| `pom.xml` | Spark maintenance jar dependencies and shade config |
| `kafka-connect/pom.xml` | Build-only Kafka Connect plugin dependency collector |
| `Dockerfile` | Spark maintenance image |
| `Dockerfile.connect` | Kafka Connect ingest image |
| `k8s/maintenance-cron.yaml` | Two Kubernetes CronJobs for Spark maintenance |
| `scripts/minikube-up.sh` / `scripts/minikube-down.sh` | Bring up / tear down minikube stack |
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
* **One catalog, two writers.** Kafka Connect and Spark must describe the same
  JDBC catalog, catalog name (`iceberg`), warehouse, S3 endpoint, credentials,
  and table: `db.events`.
* **Maintenance jobs must share the same lock key.** Both Spark jobs call
  `IcebergCatalog.withMaintenanceLock()`, which uses Postgres advisory lock
  key `db.events`. Do not give orphan GC a different coordination key.
* **Orphan cleanup stays separate.** Keep `DeleteOrphanFiles` in its own
  CronJob so it has an isolated failure domain and can be scheduled less often.
* **Delete-orphan age must exceed ingest commit cadence.** The connector commit
  interval is `iceberg.control.commit.interval-ms=60000`; orphan cleanup uses a
  10 minute age threshold. Do not shrink it near the commit cadence.
* **Hadoop client jars stay bundled.** Iceberg catalog loading needs
  `org.apache.hadoop.conf.Configuration` even with `S3FileIO`.
* **Postgres JDBC driver stays bundled.** Both the Spark maintenance jar and
  the Kafka Connect plugin need it for the JDBC catalog.
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

Useful commands:

```bash
kubectl -n s3-table-dump get pods,cronjob,jobs
kubectl -n s3-table-dump logs deploy/datagen -f
kubectl -n s3-table-dump port-forward svc/kafka-connect 8083:8083
curl -s localhost:8083/connectors/iceberg-sink/status | jq
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
