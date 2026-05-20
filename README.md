# Kafka S3 Iceberg Dump

A demo Apache Flink (Java) application that **continuously streams JSON events
from Kafka into an Apache Iceberg table on S3 (MinIO)**, with **Iceberg table
maintenance** (data-file compaction, snapshot expiration and orphan-file
removal) running as **separate Flink jobs woken on a schedule**, using the
[Flink table maintenance API](https://iceberg.apache.org/docs/nightly/flink-maintenance/).

It is deployed as **three independent Flink Application-Mode clusters** (via
the Flink Kubernetes Operator) — `ingest`, `maintenance` (rewrite + expire)
and `orphan-gc` (orphan-file removal) — each its own JobManager + TaskManager.
That gives them independent lifecycles, failure domains **and isolated
memory/CPU budgets**. Only `ingest` runs continuously; the two maintenance
clusters ship **suspended** and are woken on a schedule by Kubernetes
`CronJob`s (`k8s/maintenance-cron.yaml`) — outside their wake windows they
have zero JM/TM pods and consume no resources. Orphan GC shares the *same*
Postgres maintenance lock as `maintenance`, so when the two wake windows
happen to overlap the lock serialises orphan removal against
`ExpireSnapshots` (which deletes files) — defence in depth against the S3-404
race.

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
  │  cluster: ingest   (own JM+TM pods) │  ← always running
  │  KafkaSource ▶ JSON→RowData ▶ IcebergSink ─┐
  └────────────────────────────────────┘       │ commits
                                                ▼
  ┌────────────────────────────────────┐  Iceberg table db.events
  │  cluster: maintenance (suspended)   │  warehouse on MinIO (S3)
  │    ↑ JM+TM pods only during a wake  ▲        │
  │    • RewriteDataFiles (compaction)  └────────┘ rewrites / expires
  │    • ExpireSnapshots  (expiry)               │
  └────────────────────────────────────┘        │
  ┌────────────────────────────────────┐        │
  │  cluster: orphan-gc   (suspended)   │────────┘ deletes orphans
  │    ↑ JM+TM pods only during a wake  │
  │  TableMaintenance: DeleteOrphanFiles│
  └────────────────────────────────────┘
   ▲                                  ▲
   │ wake every 30m, sleep 180s       │ wake every 6h, sleep 180s
   │   CronJob maintenance-runner     │   CronJob orphan-gc-runner
   └──────── k8s/maintenance-cron.yaml ─────────┘
       (patches spec.job.state running⇄suspended)

   maintenance & orphan-gc share ONE Postgres lock (id db.events)
   → if wake windows overlap, the lock serialises orphan GC vs expire/rewrite
        catalog + maintenance lock: Postgres (Iceberg JDBC catalog)
```

All three clusters run the **same image** (`IcebergCatalog` holds the shared
schema/catalog/lock wiring); the Flink Kubernetes Operator runs each as its
own Application-Mode cluster from one `FlinkDeployment` each. The
Postgres-backed `JdbcLockFactory` (lock id `db.events`) is honoured **across
all clusters**: it keeps maintenance/orphan-GC from colliding with ingest
commits *and*, because `maintenance` and `orphan-gc` take the **same** lock
id, serialises orphan removal against `ExpireSnapshots`'s deletes whenever
the two wake windows overlap.

* **Isolation:** ingest, maintenance and orphan-gc each get separate
  JobManager/TaskManager pods — independent memory/CPU budgets and failure
  domains (one job's OOM/crash cannot starve or take down another).
* **Scheduled maintenance:** the `maintenance` and `orphan-gc`
  `FlinkDeployment`s ship with `spec.job.state: suspended`; the
  `maintenance-runner` and `orphan-gc-runner` `CronJob`s in
  [`k8s/maintenance-cron.yaml`](k8s/maintenance-cron.yaml) flip them to
  `running` on schedule, sleep for the wake window (default 3 min), then
  flip back to `suspended`. Outside wake windows those two clusters have
  **zero** JM/TM pods, freeing minikube RAM for ingest + infra.
* **Catalog:** Iceberg **JDBC catalog** on Postgres; the same Postgres backs
  the maintenance `JdbcLockFactory`.
* **Storage:** MinIO via Iceberg `S3FileIO` (path-style, `s3://warehouse`).
* **Table:** `db.events`, identity-partitioned by `event_type` so the stream
  produces many small files for `RewriteDataFiles` to compact.

## Prerequisites

* **Docker**, **minikube**, **kubectl**, **helm**.
* **~8 GB of memory available to Docker.** `scripts/minikube-up.sh` starts
  minikube with `--memory=7600` by default (override with `MINIKUBE_MEMORY`;
  the request must stay under the memory Docker exposes — minikube refuses
  otherwise).
  In steady state only the ingest cluster + Kafka + MinIO + Postgres + the
  operator are running; the two maintenance clusters spin up only inside a
  cron-triggered wake window. **Memory is genuinely tight** when a wake
  overlaps ingest — both maintenance and orphan-gc JM/TM pods can briefly
  exist alongside ingest's. Per-cluster Flink memory is tuned lean (JM 512m
  / TM 768m, with Flink's metaspace/overhead/managed components explicitly
  shrunk in `flinkConfiguration` so those small sizes are valid).

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
   (rewrite+expire) and `orphan-gc`. The maintenance pair starts
   **suspended**.
6. Apply [`k8s/maintenance-cron.yaml`](k8s/maintenance-cron.yaml) — RBAC +
   two `CronJob`s that wake the maintenance clusters on their schedules.

First run downloads the minikube base image + Maven dependencies, so give it
several minutes.

## Verify it works

**The three isolated clusters** — separate JM/TM pods per job is the point.
Only `ingest` runs continuously; `maintenance` and `orphan-gc` have pods
only while a cron-triggered wake is in progress:

```bash
kubectl -n s3-table-dump get flinkdeployment
kubectl -n s3-table-dump get pods   # ingest-* JM+TM always; maintenance/orphan-gc only during a wake
kubectl -n s3-table-dump get cronjob
```

`ingest` should reach `JOB STATUS: RUNNING` / `LIFECYCLE STATE: STABLE`.
`maintenance` and `orphan-gc` will show `JOB STATUS: SUSPENDED` between
wakes — that is the desired steady state.

**Force a wake now** instead of waiting for cron (useful for verification):

```bash
kubectl -n s3-table-dump create job --from=cronjob/maintenance-runner maintenance-now
kubectl -n s3-table-dump create job --from=cronjob/orphan-gc-runner orphan-gc-now
```

Each creates a one-off pod that flips the target `FlinkDeployment` to
`running`, sleeps the wake window, and flips it back to `suspended`.

**Flink Web UIs** (one per cluster — maintenance/orphan-gc UIs only respond
while pods are up):

```bash
kubectl -n s3-table-dump port-forward svc/ingest-rest 8081:8081       # ingest
kubectl -n s3-table-dump port-forward svc/maintenance-rest 8082:8081  # rewrite+expire (mid-wake only)
kubectl -n s3-table-dump port-forward svc/orphan-gc-rest 8083:8081    # orphan GC (mid-wake only)
```

**MinIO console** — `kubectl -n s3-table-dump port-forward svc/minio 9001:9001`
then http://localhost:9001 (user `admin`, pass `password`). Browse
`warehouse/db/events/`:
* `data/event_type=*/…parquet` — count **grows** as events arrive, **drops**
  after a compaction wake (RewriteDataFiles, triggered when the table has ≥3
  commits in history).
* `metadata/` — old snapshots pruned by ExpireSnapshots (keep last 3, ≤5 min).
* Unreferenced files from rewrite/expire are removed by DeleteOrphanFiles
  (only files older than its 10 min `minAge` — the safety margin, not a bug).

**Maintenance logs** (TaskManager pod of the maintenance cluster, while a
wake is active):

```bash
kubectl -n s3-table-dump logs -l app=maintenance,component=taskmanager \
  --tail=-1 | grep -Ei "rewrite|expire|orphan|TaskResult"
```

**CronJob logs** (each wake produces a Job → a Pod whose stdout is the
patch/sleep/patch script):

```bash
kubectl -n s3-table-dump logs -l job-name=maintenance-now --tail=-1
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

* `DeleteOrphanFiles.scheduleOnInterval(20 s)` / `minAge(10 min)` / `usePrefixListing(true)`
* `TableMaintenance.rateLimit(1 min)` — Iceberg gates the first trigger fire
  after a stateless cold start by `max(interval, rateLimit)`, so both values
  MUST be shorter than the cron wake window or the pass never fires. With
  a 180 s wake, ~60 s elapses before the first orphan pass; a second pass
  may run before suspend (idempotent, so harmless).
* Same `IcebergCatalog.lockFactory()` (lock id `db.events`) as the maintenance
  job → the Postgres lock serialises it against rewrite/expire when wake
  windows overlap.

Cron schedule + wake duration live in
[`k8s/maintenance-cron.yaml`](k8s/maintenance-cron.yaml):

* `schedule:` — `*/30 * * * *` for rewrite/expire, `0 */6 * * *` for orphan GC.
* The `sleep` value inside each CronJob's command is the wake window
  (default 180 s). Bump it if your minikube is slow enough that JM+TM
  cold-start eats too much of the window before the in-job triggers tick.
* `concurrencyPolicy: Forbid` — overlapping wakes against the same
  FlinkDeployment can't happen.

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
* **Resource isolation achieved + scheduled maintenance:** each job is its
  own Application cluster with its own JM+TM pods and memory/CPU budget — a
  maintenance OOM/crash no longer affects ingest. The two maintenance
  clusters additionally ship suspended and only have pods during a
  cron-triggered wake, so steady-state RAM use is essentially just ingest +
  infra. During an overlapping wake the footprint is still real, hence
  `--memory=7600`. If pods sit `Pending` during a wake, raise
  `MINIKUBE_MEMORY` or lower the `jobManager`/`taskManager` `resource` in
  the `FlinkDeployment`s.
* Iceberg's `CatalogLoader` requires `org.apache.hadoop.conf.Configuration` on
  the classpath even with `S3FileIO`; the shaded `hadoop-client-api/runtime`
  uber jars are bundled to satisfy that without dragging in a full Hadoop tree.
* The maintenance jobs' `JdbcLockFactory.open()` (run on the *TaskManager*
  operator) could lose a cold-connect race to Postgres
  (`UncheckedSQLException: Failed to connect`). With cron-driven wakes this
  matters on **every** wake, not just first deploy — each wake is a
  stateless cold start. Defence in depth, in order of who actually fixes it:
  1. **`RetryingTriggerLockFactory`** (the real fix) wraps the lock factory and
     retries `open()` *in-place on the TaskManager* (5 × 3 s), absorbing the
     transient with no job restart.
  2. **`IcebergCatalog.awaitJdbc()`** is a *fail-fast* pre-flight in the
     JobManager pod (clear error if Postgres is genuinely down).
  3. **`restart-strategy.type: fixed-delay`** (3 attempts, 5 s, set in each
     `FlinkDeployment`) is the bounded last resort.

  Expected per-wake: **`numRestarts=0`** and empty exception history. A
  *growing* exception history or a restart loop is the real signal something
  is wrong.
* Re-applying a `FlinkDeployment` triggers an operator-managed redeploy of
  that one cluster only; for a clean restart use `scripts/minikube-down.sh`
  then `scripts/minikube-up.sh`.

### Design note — maintenance is cron-driven, not continuous

`maintenance` and `orphan-gc` ship with `spec.job.state: suspended` and
`upgradeMode: stateless`. Two Kubernetes `CronJob`s in
[`k8s/maintenance-cron.yaml`](k8s/maintenance-cron.yaml) (running as the
`maintenance-runner` ServiceAccount, RBAC scoped to
`get`+`patch flinkdeployments` in the namespace) flip `spec.job.state` to
`running`, sleep for the wake window, then flip it back to `suspended`. The
Flink Kubernetes Operator reconciles those state changes by creating and
deleting the JM+TM pods. **Steady-state pod count for those two clusters is
zero.**

Why this is safe with stateless wakes: rewrite/expire use
`scheduleOnCommitCount`, which counts against the table's **all-time**
snapshot history (per Iceberg's `MonitorSource`), so the trigger fires on
the first source tick after wake without needing any persisted "since job
start" state. Orphan GC uses `scheduleOnInterval(20 s)`, kept shorter than
the wake window because Iceberg waits the full interval of wall-clock from
cold start before its first trigger; orphan GC's `rateLimit(5 min)` is in
turn longer than the wake window, so exactly one orphan pass runs per wake.
A wake interrupted mid-commit is harmless — Iceberg metadata commits are
atomic, and any half-deleted files become orphans cleaned up later. A wake
interrupted **mid-lock-hold** is less benign: Iceberg's `JdbcLockFactory`
(table `flink_maintenance_lock`) has no expiry/heartbeat, so a SIGKILL'd
TaskManager leaves a stale row that blocks future maintenance until cleared
manually:

```bash
kubectl -n s3-table-dump exec deploy/postgres -- \
  psql -U iceberg -d iceberg -c \
  "DELETE FROM flink_maintenance_lock WHERE LOCK_ID='db.events';"
```

In normal operation this isn't reached: maintenance tasks finish in seconds,
well inside the 180 s wake window, so the lock is released before the cron
suspends the cluster. The risk is only real if a task runs long enough to
still be holding the lock when `state: suspended` is patched.

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
around each maintenance cycle, so when the wake windows overlap the lock
factory **serialises orphan GC against rewrite/expire** — they can no longer
delete files underneath each other — while orphan GC still gets its own
isolated cluster (own JM+TM, independent failure domain), consistent with
the rest of the design. The default cron schedules (rewrite/expire every 30
min, orphan GC every 6 h) also make overlap rare in the first place.

Validated end-to-end on minikube: all three clusters reach
`RUNNING/STABLE` during their wake windows, `numRestarts=0`,
exception-history empty (0 entries) per wake; `orphan-gc` is observed to
take the shared lock (`Created/Deleted JdbcLock{type=MAINTENANCE,
lockId=db.events}` in its TM logs). The shared-lock serialisation path
therefore works as designed.

What the validation *also* showed: the underlying Iceberg-Flink
`DeleteOrphanFiles` ↔ `S3FileIO`-on-MinIO interaction can **still** surface a
`NoSuchKeyException` (404) on `BaseS3File.getObjectMetadata`, because
`ExpireSnapshots`' bulk-delete tail can continue past the lock release window
and the orphan detector lists then HEADs files. That's an Iceberg-operator-
level race we can't fix from outside the operator. It remains **contained**:
job stays `RUNNING`, `numRestarts=0`, exception-history empty, only the
individual orphan pass is `TaskResult{success=false}`, and it retries on the
next cron wake.
