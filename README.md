# Kafka S3 Iceberg Dump

A demo Apache Flink (Java) application that **continuously streams JSON events
from Kafka into an Apache Iceberg table on S3 (MinIO)**, with **Iceberg table
maintenance** (data-file compaction, snapshot expiration and orphan-file
removal) running as a **separate, self-triggering Flink job**, using the
[Flink table maintenance API](https://iceberg.apache.org/docs/nightly/flink-maintenance/).

It is deployed as **three independent Flink Application-Mode clusters** (via
the Flink Kubernetes Operator) — `ingest`, `maintenance` (rewrite + expire)
and `orphan-gc` (orphan-file removal) — each its own JobManager + TaskManager.
That gives them independent lifecycles, failure domains **and isolated
memory/CPU budgets**. Orphan GC is split out and shares the *same* Postgres
maintenance lock as `maintenance`, so the lock serialises it against
`ExpireSnapshots` (which deletes files) — fixing the S3-404 race without
giving up isolation.

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
| Flink Kubernetes Operator | 1.14.0 | manages the three Application-Mode clusters |
| Maven | 3.9.9 | runs in the Docker build stage; no local JDK/Maven needed |
| MinIO / Postgres | alpine/minio RELEASE.2025-10-15 / pg 17.10 | S3 storage / Iceberg JDBC catalog + maintenance lock |
| minikube / kubectl / helm | any recent | local Kubernetes + deploy tooling |

## Architecture

```
 datagen ──JSON──▶ Kafka topic "events"
                        │
                        ▼
  ┌────────────────────────────────────┐
  │  cluster: ingest   (own JM+TM pods) │
  │  KafkaSource ▶ JSON→RowData ▶ IcebergSink ─┐
  └────────────────────────────────────┘       │ commits
                                                ▼
  ┌────────────────────────────────────┐  Iceberg table db.events
  │  cluster: maintenance (own JM+TM)   │  warehouse on MinIO (S3)
  │  TableMaintenance, self-triggering: ▲        │
  │    • RewriteDataFiles (compaction)  └────────┘ rewrites / expires
  │    • ExpireSnapshots  (expiry)               │
  └────────────────────────────────────┘        │
  ┌────────────────────────────────────┐        │
  │  cluster: orphan-gc (own JM+TM)     │────────┘ deletes orphans
  │  TableMaintenance: DeleteOrphanFiles│
  └────────────────────────────────────┘
   maintenance & orphan-gc share ONE Postgres lock (id db.events)
   → lock serialises orphan GC vs expire/rewrite (no S3-404 race)
        catalog + maintenance lock: Postgres (Iceberg JDBC catalog)
```

All three clusters run the **same image** (`IcebergCatalog` holds the shared
schema/catalog/lock wiring); the Flink Kubernetes Operator runs each as its
own Application-Mode cluster from one `FlinkDeployment` each. The
Postgres-backed `JdbcLockFactory` (lock id `db.events`) is honoured **across
all clusters**: it keeps maintenance/orphan-GC from colliding with ingest
commits *and*, because `maintenance` and `orphan-gc` take the **same** lock
id, serialises orphan removal against `ExpireSnapshots`'s deletes.

* **Isolation:** ingest, maintenance and orphan-gc each get separate
  JobManager/TaskManager pods — independent memory/CPU budgets and failure
  domains (one job's OOM/crash cannot starve or take down another).
* **Catalog:** Iceberg **JDBC catalog** on Postgres; the same Postgres backs
  the maintenance `JdbcLockFactory`.
* **Storage:** MinIO via Iceberg `S3FileIO` (path-style, `s3://warehouse`).
* **Table:** `db.events`, identity-partitioned by `event_type` so the stream
  produces many small files for `RewriteDataFiles` to compact.

## Prerequisites

* **Docker**, **minikube**, **kubectl**, **helm**.
* **~8 GB of memory available to Docker.** `scripts/minikube-up.sh` starts
  minikube with `--memory=8000` by default (override with `MINIKUBE_MEMORY`)
  to fit **three** Flink clusters + Kafka + MinIO + Postgres + the operator.
  Per-cluster Flink memory is tuned lean (JM 512m / TM 768m, with Flink's
  metaspace/overhead/managed components explicitly shrunk in
  `flinkConfiguration` so those small sizes are valid); true resource
  isolation costs more RAM than a shared cluster — that is the tradeoff being
  demonstrated, and on an 8 GB box this footprint is genuinely tight.

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
5. Apply the three `FlinkDeployment`s — `ingest`, `maintenance`
   (rewrite+expire) and `orphan-gc`.

First run downloads the minikube base image + Maven dependencies, so give it
several minutes.

## Verify it works

**The three isolated clusters** — separate JM/TM pods per job is the point:

```bash
kubectl -n s3-table-dump get flinkdeployment
kubectl -n s3-table-dump get pods   # ingest-* / maintenance-* / orphan-gc-* JM+TM
```

All three `FlinkDeployment`s should reach `JOB STATUS: RUNNING` /
`LIFECYCLE STATE: STABLE`.

**Flink Web UIs** (one per cluster — three separate UIs by design):

```bash
kubectl -n s3-table-dump port-forward svc/ingest-rest 8081:8081       # ingest
kubectl -n s3-table-dump port-forward svc/maintenance-rest 8082:8081  # rewrite+expire
kubectl -n s3-table-dump port-forward svc/orphan-gc-rest 8083:8081    # orphan GC
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

Rewrite/expire behaviour is in
[`IcebergMaintenanceJob.java`](src/main/java/com/example/flinkiceberg/IcebergMaintenanceJob.java):

* `RewriteDataFiles.scheduleOnCommitCount(3)` / `targetFileSizeBytes(64MB)`
* `ExpireSnapshots.scheduleOnCommitCount(5)` / `maxSnapshotAge(5 min)` / `retainLast(3)`
* `TableMaintenance.rateLimit(1 min)` / `lockCheckDelay(10 s)`

Orphan-GC behaviour is in
[`IcebergOrphanGcJob.java`](src/main/java/com/example/flinkiceberg/IcebergOrphanGcJob.java):

* `DeleteOrphanFiles.scheduleOnInterval(15 min)` / `minAge(10 min)` / `usePrefixListing(true)`
* Same `IcebergCatalog.lockFactory()` (lock id `db.events`) as the maintenance
  job → the Postgres lock serialises it against rewrite/expire.

Ingest behaviour (Kafka source, checkpoint interval) is in
[`KafkaToIcebergJob.java`](src/main/java/com/example/flinkiceberg/KafkaToIcebergJob.java).
Both jobs read all connection settings from environment variables with
defaults that match the in-cluster Service names (`kafka`, `postgres`,
`minio`) — see
[`IcebergCatalog.fromEnv()`](src/main/java/com/example/flinkiceberg/IcebergCatalog.java),
so only `S3_REGION` is set explicitly in the `FlinkDeployment`s. Per-cluster
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

### Design note — orphan GC is isolated and lock-serialised

Earlier, `DeleteOrphanFiles` ran in the same `TableMaintenance` graph as
`ExpireSnapshots`. Orphan detection HEADs candidate files
(`BaseS3File.getObjectMetadata`); when `ExpireSnapshots` deleted
snapshot/manifest files in the same window, the HEAD hit a transient **S3 404
(`NoSuchKeyException`)** and that orphan pass reported
`TaskResult{success=false}` — inherent to running orphan removal beside
another file-deleter in one graph.

The fix: orphan removal now lives in its own job/cluster
(`IcebergOrphanGcJob` → the `orphan-gc` `FlinkDeployment`) that uses the
**same** Postgres maintenance lock (`IcebergCatalog.lockFactory()`, lock id
`db.events`) as the `maintenance` job. `TableMaintenance` acquires that lock
around each maintenance cycle, so the lock factory **serialises orphan GC
against rewrite/expire** — they can no longer delete files underneath each
other — while orphan GC still gets its own isolated cluster (own JM+TM,
independent failure domain), consistent with the rest of the design. Orphan GC
also runs infrequently (`scheduleOnInterval(15 min)`) so it mostly finds the
lock free.

Validated end-to-end on minikube: all three clusters `RUNNING/STABLE`,
`numRestarts=0`, exception-history empty (0 entries) for all jobs;
`orphan-gc` is observed to take the shared lock
(`Created/Deleted JdbcLock{type=MAINTENANCE, lockId=db.events}` in its TM
logs). The shared-lock serialisation path therefore works as designed.

What the validation *also* showed: the underlying Iceberg-Flink
`DeleteOrphanFiles` ↔ `S3FileIO`-on-MinIO interaction can **still** surface a
`NoSuchKeyException` (404) on `BaseS3File.getObjectMetadata`, because
`ExpireSnapshots`' bulk-delete tail can continue past the lock release window
and the orphan detector lists then HEADs files. That's an Iceberg-operator-
level race we can't fix from outside the operator. It remains **contained**:
job stays `RUNNING`, `numRestarts=0`, exception-history empty, only the
individual orphan pass is `TaskResult{success=false}`, and it retries on the
next 15-min interval.
