# Kafka S3 Iceberg Dump

A demo Apache Flink (Java) application that **continuously streams JSON events
from Kafka into an Apache Iceberg table on S3 (MinIO)**, with **Iceberg table
maintenance** (data-file compaction, snapshot expiration and orphan-file
removal) running as a **separate, self-triggering Flink job**, using the
[Flink table maintenance API](https://iceberg.apache.org/docs/nightly/flink-maintenance/).

Ingest and maintenance are deployed as **two independent Flink Application-Mode
clusters** (via the Flink Kubernetes Operator) — each its own JobManager +
TaskManager. That gives them independent lifecycles, failure domains **and
isolated memory/CPU budgets** (the reason for moving off a single shared
session cluster).

The whole thing — image build, operator, storage and a data generator — comes
up on **minikube** with a single `scripts/minikube-up.sh`.

## Stack (latest, mutually compatible — May 2026)

| Component | Version | Notes |
|---|---|---|
| Java | 21 | latest LTS supported by Flink 2.0 |
| Apache Flink | 2.0.2 | Iceberg ships a Flink connector for the 2.0 line |
| flink-connector-kafka | 4.0.1-2.0 | externalized Kafka connector for Flink 2.0 |
| Apache Iceberg | 1.10.2 | `iceberg-flink-runtime-2.0` + `iceberg-aws-bundle` |
| Apache Kafka | 4.0.0 | KRaft mode (no ZooKeeper) |
| Flink Kubernetes Operator | 1.14.0 | manages the two Application-Mode clusters |
| Maven | 3.9.9 | runs in the Docker build stage; no local JDK/Maven needed |
| MinIO / Postgres | alpine/minio RELEASE.2025-10-15 / pg 17.10 | S3 storage / Iceberg JDBC catalog + maintenance lock |
| minikube / kubectl / helm | any recent | local Kubernetes + deploy tooling |

## Architecture

```
 datagen ──JSON──▶ Kafka topic "events"
                        │
                        ▼
  ┌────────────────────────────────────┐
  │  Flink Application cluster: ingest  │   own JobManager + TaskManager pods
  │  KafkaSource ▶ JSON→RowData ▶ IcebergSink ─┐
  └────────────────────────────────────┘       │ commits
                                                ▼
  ┌────────────────────────────────────┐  Iceberg table db.events
  │  Flink Application cluster:         │  warehouse on MinIO (S3)
  │  maintenance   (own JM + TM pods)   ▲        │
  │  TableMaintenance, self-triggering: │        │ reads commits,
  │    • RewriteDataFiles  (compaction) └────────┘ rewrites, expires,
  │    • ExpireSnapshots   (expiry)              │ deletes orphans
  │    • DeleteOrphanFiles (orphan GC)           │
  └────────────────────────────────────┘        │
        catalog + maintenance lock: Postgres (Iceberg JDBC catalog)
```

Both clusters run the **same image** (`IcebergCatalog` holds the shared
schema/catalog/lock wiring); the Flink Kubernetes Operator runs each as its
own Application-Mode cluster from one `FlinkDeployment` each. The
Postgres-backed `JdbcLockFactory` is honoured **across the two clusters**, so
compaction, expiration and orphan-file removal never collide with ingest
commits.

* **Isolation:** ingest and maintenance get separate JobManager/TaskManager
  pods — independent memory/CPU budgets and failure domains (a maintenance
  OOM/crash cannot starve or take down ingest).
* **Catalog:** Iceberg **JDBC catalog** on Postgres; the same Postgres backs
  the maintenance `JdbcLockFactory`.
* **Storage:** MinIO via Iceberg `S3FileIO` (path-style, `s3://warehouse`).
* **Table:** `db.events`, identity-partitioned by `event_type` so the stream
  produces many small files for `RewriteDataFiles` to compact.

## Prerequisites

* **Docker**, **minikube**, **kubectl**, **helm**.
* **~8 GB of memory available to Docker.** `scripts/minikube-up.sh` starts
  minikube with `--memory=7600` by default (override with `MINIKUBE_MEMORY`)
  to fit two Flink clusters + Kafka + MinIO + Postgres + the operator. True
  resource isolation costs more RAM than the old single-cluster setup — that
  is the tradeoff being demonstrated.

## Quick start

```bash
scripts/minikube-up.sh
```

That will, in order:

1. `minikube start` (sized for the workload), if not already running.
2. Build `s3-table-dump:dev` **inside minikube's docker** (multi-stage
   Dockerfile: Maven build stage → Flink 2.0 image with the jar in
   `/opt/flink/usrlib`). No local JDK/Maven needed.
3. Install the **Flink Kubernetes Operator** (Helm, `webhook.create=false` to
   skip cert-manager; `watchNamespaces` makes it create the `flink` RBAC).
4. Deploy infra: `postgres`, `minio` (+ a `bucket-setup` sidecar that creates
   the `warehouse` bucket), `kafka` (KRaft, auto-creates `events` with 3
   partitions), `datagen`.
5. Apply the two `FlinkDeployment`s — `ingest` and `maintenance`.

First run downloads the minikube base image + Maven dependencies, so give it
several minutes.

## Verify it works

**The two isolated clusters** — separate JM/TM pods per job is the point:

```bash
kubectl -n s3-table-dump get flinkdeployment
kubectl -n s3-table-dump get pods            # ingest-* and maintenance-* JM/TM
```

Both `FlinkDeployment`s should reach `JOB STATUS: RUNNING` /
`LIFECYCLE STATE: STABLE`.

**Flink Web UIs** (one per cluster — two separate UIs by design):

```bash
kubectl -n s3-table-dump port-forward svc/ingest-rest 8081:8081       # ingest
kubectl -n s3-table-dump port-forward svc/maintenance-rest 8082:8081  # maintenance
```

**MinIO console** — `kubectl -n s3-table-dump port-forward svc/minio 9001:9001`
then http://localhost:9001 (user `admin`, pass `password`). Browse
`warehouse/db/events/`:
* `data/event_type=*/…parquet` — count **grows** as events arrive, **drops**
  after a compaction run (RewriteDataFiles, every 3 commits).
* `metadata/` — old snapshots pruned by ExpireSnapshots (keep last 3, ≤5 min).
* Unreferenced files from rewrite/expire are removed by DeleteOrphanFiles
  (only files older than its 10 min `minAge` — the safety margin, not a bug).

**Maintenance logs** (TaskManager pod of the maintenance cluster):

```bash
kubectl -n s3-table-dump logs -l app=maintenance,component=taskmanager \
  --tail=-1 | grep -Ei "rewrite|expire|orphan|TaskResult"
```

**Catalog & lock in Postgres**

```bash
kubectl -n s3-table-dump exec deploy/postgres -- \
  psql -U iceberg -d iceberg -c "\dt" -c "select * from iceberg_tables;"
```

## Tuning

Maintenance behaviour is set in
[`IcebergMaintenanceJob.java`](src/main/java/com/example/flinkiceberg/IcebergMaintenanceJob.java):

* `RewriteDataFiles.scheduleOnCommitCount(3)` / `targetFileSizeBytes(64MB)`
* `ExpireSnapshots.scheduleOnCommitCount(5)` / `maxSnapshotAge(5 min)` / `retainLast(3)`
* `DeleteOrphanFiles.scheduleOnInterval(30 min)` / `minAge(10 min)` / `usePrefixListing(true)` (infrequent — minimises, but cannot eliminate, the race with ExpireSnapshots; see *Known limitation* below)
* `TableMaintenance.rateLimit(1 min)` / `lockCheckDelay(10 s)`

Ingest behaviour (Kafka source, checkpoint interval) is in
[`KafkaToIcebergJob.java`](src/main/java/com/example/flinkiceberg/KafkaToIcebergJob.java).
Both jobs read all connection settings from environment variables with
defaults that match the in-cluster Service names (`kafka`, `postgres`,
`minio`) — see
[`IcebergCatalog.fromEnv()`](src/main/java/com/example/flinkiceberg/IcebergCatalog.java),
so only `AWS_REGION` is set explicitly in the `FlinkDeployment`s. Per-cluster
CPU/memory live in [`k8s/flink-ingest.yaml`](k8s/flink-ingest.yaml) and
[`k8s/flink-maintenance.yaml`](k8s/flink-maintenance.yaml).

## Build / run locally (without Kubernetes)

```bash
./mvnw -DskipTests package          # produces target/app.jar (0.0.0-SNAPSHOT)

# ingest job (jar manifest main class):
flink run target/app.jar
# maintenance job (override the main class):
flink run -c com.example.flinkiceberg.IcebergMaintenanceJob target/app.jar
```

To stamp a real version, pass `-Drevision` from git-semver-release — see
[Versioning](#versioning). Override connection defaults via env vars, e.g.
`KAFKA_BOOTSTRAP=localhost:9092`.

## Teardown

```bash
scripts/minikube-down.sh                 # deletes the whole minikube profile
scripts/minikube-down.sh --keep-cluster  # only removes the app + operator
```

## Versioning

Releases are tagged with
[git-semver-release](https://github.com/michalstutzmann/git-semver-release) — a
zero-dependency Bash tool for SemVer release tagging. We use **manual bumps**;
pick the bump level explicitly per release.

```bash
git-semver-release version          # show current version
git-semver-release patch --dry-run  # preview the next tag
git-semver-release minor --push     # tag + push the release
```

It is also available via Homebrew
(`brew install michalstutzmann/git-semver-release/git-semver-release`) or
Docker (`docker run --rm -v "$PWD:/home"
ghcr.io/michalstutzmann/git-semver-release version`).

### Build version (Maven CI-friendly)

The `pom.xml` uses a [Maven CI-friendly
version](https://maven.apache.org/guides/mini/guide-maven-ci-friendly.html):
`<version>${revision}</version>`, defaulting to `0.0.0-SNAPSHOT`. The version is
**never hardcoded** — git-semver-release is the single source of truth and
feeds Maven via `-Drevision`:

```bash
./mvnw -DskipTests -Drevision="$(git-semver-release version)" package
```

`scripts/minikube-up.sh` picks this up automatically: it passes
`APP_VERSION="$(git-semver-release version)"` (falling back to
`0.0.0-SNAPSHOT`) as the Dockerfile `--build-arg`, which the build stage
forwards to `-Drevision`. Override with `APP_VERSION=… scripts/minikube-up.sh`.

## Notes / troubleshooting

* **All images/charts are pinned** for reproducibility (`k8s/*`,
  `Dockerfile`, `OPERATOR_VERSION` in `scripts/minikube-up.sh`). MinIO
  archived its own Docker Hub repo in 2025, so everything MinIO-related uses
  the maintained `alpine/minio` rebuild (runs as root via `runAsUser: 0` to
  write the `emptyDir`). No `mc` image: a `bucket-setup` **sidecar** in the
  minio pod shares the data volume and `mkdir`s `/data/warehouse` once minio
  has formatted the drive (a top-level dir *is* a bucket in MinIO's
  single-drive backend).
* In **Application Mode** the job's `main()` runs in the **JobManager pod**
  (no separate submitter), so `IcebergCatalog.awaitJdbc()` /
  `ensureTable()` run there; `ensureTable()` is idempotent and race-tolerant,
  so the two clusters can come up in any order.
* **Resource isolation achieved:** each job is its own Application cluster
  with its own JM+TM pods and memory/CPU budget — a maintenance OOM/crash no
  longer affects ingest. The cost is RAM: two clusters need materially more
  than the old single shared TaskManager (hence `--memory=7600`). If pods sit
  `Pending`, raise `MINIKUBE_MEMORY` or lower the `jobManager`/`taskManager`
  `resource` in the `FlinkDeployment`s.
* Iceberg's `CatalogLoader` requires `org.apache.hadoop.conf.Configuration` on
  the classpath even with `S3FileIO`; the shaded `hadoop-client-api/runtime`
  uber jars are bundled to satisfy that without dragging in a full Hadoop tree.
* The maintenance job's `JdbcLockFactory.open()` (run on the *TaskManager*
  operator) could lose a cold-connect race to Postgres on first deploy
  (`UncheckedSQLException: Failed to connect`), costing one startup restart.
  Defence in depth, in order of who actually fixes it:
  1. **`RetryingTriggerLockFactory`** (the real fix) wraps the lock factory and
     retries `open()` *in-place on the TaskManager* (5 × 3 s), absorbing the
     transient with no job restart.
  2. **`IcebergCatalog.awaitJdbc()`** is a *fail-fast* pre-flight in the
     JobManager pod (clear error if Postgres is genuinely down).
  3. **`restart-strategy.type: fixed-delay`** (3 attempts, 5 s, set in each
     `FlinkDeployment`) is the bounded last resort.

  Expected steady state: **`numRestarts=0`** for both jobs and empty exception
  history. A *growing* exception history or a restart loop is the real signal
  something is wrong.
* Re-applying a `FlinkDeployment` triggers an operator-managed redeploy of
  that one cluster only; for a clean restart use `scripts/minikube-down.sh`
  then `scripts/minikube-up.sh`.

### Known limitation — `DeleteOrphanFiles` ↔ `ExpireSnapshots` race

`DeleteOrphanFiles` lists the table prefix and HEADs candidate files
(`BaseS3File.getObjectMetadata`); when `ExpireSnapshots` deletes
snapshot/manifest files in the same window, the HEAD can hit a transient
**S3 404 (`NoSuchKeyException`)** and that orphan run reports
`TaskResult{success=false}`. This is **inherent** to running orphan removal
alongside another file-deleting task in one job — not a config bug — and is
**contained**: the maintenance *job* stays `RUNNING` with `numRestarts=0` and
no exception-history entry; only that orphan pass is skipped and it retries on
the next 30-min interval. RewriteDataFiles and ExpireSnapshots are unaffected.
Validated state: ingest + maintenance `numRestarts=0` / 0 exceptions;
RewriteDataFiles + ExpireSnapshots `success=true`; DeleteOrphanFiles
intermittently `success=false` on MinIO via this race (it succeeded under the
earlier single-JVM session run, so it is timing/state-sensitive, not broken).
For production, run orphan removal **isolated** from concurrent deleters (its
own infrequent job/window) rather than in the same `TableMaintenance` graph.
