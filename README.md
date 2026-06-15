# Kafka S3 Iceberg Dump

A minikube demo that continuously streams JSON events from Kafka into an
Apache Iceberg table on S3-compatible storage (MinIO), then runs Iceberg table
maintenance as scheduled Spark batch jobs.

It deliberately keeps ingest and maintenance separate:

* **Ingest = Apache Iceberg Kafka Connect sink connector.** Events are produced
  as JSON Schema through Confluent Schema Registry. The connector maps the
  registry schema to Iceberg columns and owns `db.events` with
  `auto-create-enabled` and `evolve-schema-enabled`.
* **Maintenance = two Spark jobs launched by Kubernetes CronJobs.**
  `maintenance-runner` runs `RewriteDataFiles` and `ExpireSnapshots`;
  `orphan-gc-runner` runs `DeleteOrphanFiles`. They are separate pods with
  separate resource budgets and both take the same Postgres advisory lock keyed
  by `db.events`, so overlapping maintenance runs serialize.

The whole stack comes up with:

```bash
scripts/minikube-up.sh
```

## Stack

| Component | Version | Notes |
|---|---:|---|
| Java | 21 | Used by Maven and the Spark runtime image |
| Apache Spark | 4.0.1 | Maintenance jobs only, local-mode driver pods |
| Apache Iceberg | 1.10.1 | Spark runtime + Kafka Connect sink kept on one Iceberg line |
| Kafka Connect + Schema Registry | Confluent Platform 8.2.1 | JSON Schema ingest path |
| Apache Kafka | 4.0.0 | Single-node KRaft broker |
| Maven | 3.9.9 | Runs inside Docker builds |
| MinIO / Postgres | alpine/minio RELEASE.2025-10-15 / pg 17.10 | S3 warehouse / JDBC catalog + advisory lock |

## Architecture

```text
datagen -> Kafka topic events -> Kafka Connect IcebergSinkConnector
                                      |
                                      v
                               Iceberg table db.events
                               warehouse: s3://warehouse on MinIO
                               catalog: Postgres JDBC catalog

maintenance-runner CronJob  -> Spark RewriteDataFiles + ExpireSnapshots
orphan-gc-runner CronJob    -> Spark DeleteOrphanFiles

Both Spark jobs use the same Postgres advisory lock:
  pg_advisory_lock(hashtext('db.events')::bigint)
```

The connector and Spark jobs share the same catalog properties:

* JDBC catalog: `jdbc:postgresql://postgres:5432/iceberg`, catalog name
  `iceberg`
* Warehouse: `s3://warehouse`
* FileIO: `org.apache.iceberg.aws.s3.S3FileIO`
* S3 endpoint: `http://minio:9000`, path-style access

The table is created by Kafka Connect, not by Spark. If a maintenance CronJob
runs before the first connector commit creates `db.events`, the job fails
clearly and the next scheduled run retries.

## Build And Run

```bash
./mvnw -DskipTests package
scripts/minikube-up.sh
scripts/minikube-down.sh
scripts/minikube-down.sh --keep-cluster
```

`scripts/minikube-up.sh` builds two images inside minikube's Docker daemon:

* `s3-table-dump:dev`: Spark 4.0.1 + the maintenance jar at
  `/opt/spark/work-dir/app.jar`.
* `s3-table-dump-connect:dev`: Confluent Kafka Connect + the Iceberg sink
  plugin and JSON Schema converter.

No image registry is needed because the Kubernetes manifests use
`imagePullPolicy: Never`.

## Verify

Watch pods and CronJobs:

```bash
kubectl -n s3-table-dump get pods,cronjob,jobs
kubectl -n s3-table-dump logs deploy/datagen -f
```

Check the connector:

```bash
kubectl -n s3-table-dump port-forward svc/kafka-connect 8083:8083
curl -s localhost:8083/connectors/iceberg-sink/status | jq
```

Check Schema Registry:

```bash
kubectl -n s3-table-dump port-forward svc/schema-registry 8081:8081
curl -s localhost:8081/subjects | jq
```

Force maintenance runs without waiting for cron:

```bash
kubectl -n s3-table-dump create job --from=cronjob/maintenance-runner maintenance-now
kubectl -n s3-table-dump create job --from=cronjob/orphan-gc-runner orphan-gc-now
kubectl -n s3-table-dump logs -l job-name=maintenance-now --tail=-1
kubectl -n s3-table-dump logs -l job-name=orphan-gc-now --tail=-1
```

Open MinIO:

```bash
kubectl -n s3-table-dump port-forward svc/minio 9001:9001
```

Then browse http://localhost:9001 with `admin` / `password`. The table lives
under `warehouse/db/events/`.

Check the JDBC catalog:

```bash
kubectl -n s3-table-dump exec deploy/postgres -- \
  psql -U iceberg -d iceberg -c "\dt" -c "select * from iceberg_tables;"
```

## Maintenance Tuning

Spark maintenance code lives in:

* [IcebergMaintenanceJob.java](src/main/java/com/example/sparkiceberg/IcebergMaintenanceJob.java)
* [IcebergOrphanGcJob.java](src/main/java/com/example/sparkiceberg/IcebergOrphanGcJob.java)
* [IcebergCatalog.java](src/main/java/com/example/sparkiceberg/IcebergCatalog.java)

Kubernetes schedules and pod resources live in
[k8s/maintenance-cron.yaml](k8s/maintenance-cron.yaml):

* `maintenance-runner`: every 30 minutes, runs rewrite + expire.
* `orphan-gc-runner`: every 6 hours, runs orphan cleanup.
* `concurrencyPolicy: Forbid`: prevents the same CronJob from overlapping
  with itself.
* Cross-job overlap is serialized by the shared Postgres advisory lock.

Current Java maintenance knobs:

* Rewrite target file size: `64 MiB`
* Rewrite partial progress: enabled, max `2` commits
* Expire snapshots: older than `5 min`, retain last `3`
* Delete orphan files: older than `10 min`, prefix listing enabled

Keep `DeleteOrphanFiles.olderThan` comfortably above the connector commit
cadence (`iceberg.control.commit.interval-ms`, currently 60 seconds) so
in-flight files are not treated as orphans.

## Schema Mapping And Evolution

`scripts/datagen.sh` registers JSON Schemas through Schema Registry and
produces JSON Schema encoded records to Kafka. The connector's
`JsonSchemaConverter` maps those schemas to Iceberg types and evolves the table.

Important details:

* `event_time` is a Kafka Connect `Timestamp` logical type encoded as epoch
  milliseconds, not an ISO string.
* JSON Schema evolution uses Schema Registry's default BACKWARD compatibility.
* The demo schemas use a closed content model
  (`"additionalProperties": false`).
* The v2 `currency` field is optional, so old records remain compatible.

## Versioning

Project releases are tagged with
[git-semver-release](https://github.com/michalstutzmann/git-semver-release)
using explicit manual bumps:

```bash
git-semver-release patch
git-semver-release minor
git-semver-release major
```

The Maven project uses a CI-friendly version:

```xml
<version>${revision}</version>
```

Do not hardcode a release number in `pom.xml`; pass it with
`-Drevision="$(git-semver-release version)"` or let `scripts/minikube-up.sh`
forward `APP_VERSION` into the Docker build.
