# Kafka S3 Iceberg Dump

A demo that **continuously streams JSON events from Kafka into an Apache
Iceberg table on S3 (MinIO)**, with **Iceberg table maintenance** (data-file
compaction, snapshot expiration and orphan-file removal) running as **separate
Flink jobs woken on a schedule**. It deliberately uses **two runtimes**:

* **Ingest runs on the [Apache Iceberg Kafka Connect sink connector](https://iceberg.apache.org/docs/nightly/kafka-connect/)**
  — *not* Flink. Events are produced as **JSON Schema** through **Confluent
  Schema Registry**; the connector's JSON Schema converter maps the registry
  schema to Iceberg columns and **owns the table** (`auto-create` +
  `evolve-schema`), so adding a field to the producer's schema **evolves the
  Iceberg table live**, with no code change or redeploy. This is the headline
  demo — see [Schema mapping & evolution](#schema-mapping--evolution).
* **Maintenance runs on two Flink Application-Mode clusters** (via the Flink
  Kubernetes Operator) — `maintenance` (rewrite + expire) and `orphan-gc`
  (orphan-file removal), using the
  [Flink table maintenance API](https://iceberg.apache.org/docs/nightly/flink-maintenance/).
  Each is its own JobManager + TaskManager — independent lifecycles, failure
  domains **and isolated memory/CPU budgets**. Both ship **suspended** and are
  woken on a schedule by Kubernetes `CronJob`s (`k8s/maintenance-cron.yaml`);
  outside their wake windows they have zero JM/TM pods. Orphan GC shares the
  *same* Postgres maintenance lock as `maintenance`, so when the two wake
  windows overlap the lock serialises orphan removal against `ExpireSnapshots`
  — defence in depth against the S3-404 race.

Ingest (Kafka Connect) and the Flink maintenance jobs **share one Iceberg
JDBC-catalog-on-Postgres table** on MinIO. The whole thing — both image
builds, operator, storage and a data generator — comes up on **minikube** with
a single `scripts/minikube-up.sh`.

## Stack (latest, mutually compatible — May 2026)

| Component | Version | Notes |
|---|---|---|
| Java | 21 | latest LTS supported by Flink 2.0 |
| Apache Iceberg | 1.10.2 | `iceberg-flink-runtime-2.0` (maintenance) + `iceberg-kafka-connect` (ingest) + `iceberg-aws-bundle` |
| Apache Flink | 2.0.2 | maintenance jobs only; Iceberg ships a Flink connector for the 2.0 line |
| Flink Kubernetes Operator | 1.14.0 | manages the two maintenance Application-Mode clusters |
| Kafka Connect + Schema Registry | Confluent Platform 8.2.1 | `cp-kafka-connect` (Iceberg sink, ingest) + `cp-schema-registry` (JSON Schema). Confluent Community License |
| Apache Kafka | 4.0.0 | KRaft mode (no ZooKeeper) |
| Maven | 3.9.9 | runs in both Docker build stages; no local JDK/Maven needed |
| MinIO / Postgres | alpine/minio RELEASE.2025-10-15 / pg 17.10 | S3 storage / Iceberg JDBC catalog + maintenance lock |
| minikube / kubectl / helm | any recent | local Kubernetes + deploy tooling |

## Architecture

```
                 registers JSON Schema
 datagen ───────────────┬───────────────▶ Schema Registry (Confluent)
   │ JSON Schema records │                        ▲
   ▼                     ▼                        │ reads schema by id
 Kafka topic "events" ───────────────────────────┤
                                                  │
  ┌───────────────────────────────────────────┐  │  ← always running
  │  Kafka Connect worker  (Iceberg sink)      │──┘
  │  JsonSchemaConverter ▶ IcebergSinkConnector│──┐ auto-create + evolve-schema
  └───────────────────────────────────────────┘  │ commits (every commit.interval-ms)
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
   │ wake every 30m, sleep 240s       │ wake every 6h, sleep 240s
   │   CronJob maintenance-runner     │   CronJob orphan-gc-runner
   └──────── k8s/maintenance-cron.yaml ─────────┘
       (patches spec.job.state running⇄suspended)

   maintenance & orphan-gc share ONE Postgres lock (id db.events)
   → if wake windows overlap, the lock serialises orphan GC vs expire/rewrite
        catalog + maintenance lock: Postgres (Iceberg JDBC catalog)
```

**Ingest** is a single Kafka Connect worker running the
`IcebergSinkConnector` (image: `cp-kafka-connect` + the Iceberg plugin +
Confluent JSON Schema converter). It reads the JSON Schema from Schema
Registry, maps it to Iceberg types, and — with `auto-create-enabled` +
`evolve-schema-enabled` — creates and evolves `db.events` itself. **Both
maintenance clusters** run the **same Flink image** (`IcebergCatalog` holds
the shared catalog/lock wiring), each its own Application-Mode cluster. The
Postgres-backed `JdbcLockFactory` (lock id `db.events`) is honoured across the
two: because `maintenance` and `orphan-gc` take the **same** lock id, it
serialises orphan removal against `ExpireSnapshots`' deletes whenever the wake
windows overlap. Ingest does not use this maintenance lock; the connector
commits via its own Iceberg-commit coordinator (optimistic concurrency).

* **Isolation:** ingest (Connect) and the two maintenance clusters each get
  separate pods — independent memory/CPU budgets and failure domains (one
  job's OOM/crash cannot starve or take down another).
* **Scheduled maintenance:** the `maintenance` and `orphan-gc`
  `FlinkDeployment`s ship with `spec.job.state: suspended`; the
  `maintenance-runner` and `orphan-gc-runner` `CronJob`s in
  [`k8s/maintenance-cron.yaml`](k8s/maintenance-cron.yaml) flip them to
  `running` on schedule, sleep for the wake window (default 240 s), then
  flip back to `suspended`. Outside wake windows those two clusters have
  **zero** JM/TM pods, freeing minikube RAM for ingest + infra.
* **Catalog:** Iceberg **JDBC catalog** on Postgres, shared by the connector
  (`iceberg.catalog.*` in `k8s/connector-setup.yaml`) and the Flink jobs
  (`IcebergCatalog.fromEnv()`); the same Postgres backs the maintenance
  `JdbcLockFactory`.
* **Storage:** MinIO via Iceberg `S3FileIO` (path-style, `s3://warehouse`).
* **Table:** `db.events`, created by the connector and identity-partitioned by
  `event_type` (`iceberg.tables.default-partition-by`) so the stream produces
  many small files for `RewriteDataFiles` to compact.

## Prerequisites

* **Docker**, **minikube**, **kubectl**, **helm**.
* **~8 GB of memory available to Docker.** `scripts/minikube-up.sh` starts
  minikube with `--memory=7600` by default (override with `MINIKUBE_MEMORY`;
  the request must stay under the memory Docker exposes — minikube refuses
  otherwise).
  In steady state only ingest (Kafka Connect + Schema Registry) + Kafka +
  MinIO + Postgres + the operator are running; the two maintenance clusters
  spin up only inside a cron-triggered wake window. **Memory is genuinely
  tight** when a wake overlaps ingest — both maintenance and orphan-gc JM/TM
  pods can briefly exist alongside the Connect worker. Per-cluster Flink
  memory is tuned lean (JM 512m / TM 768m, with Flink's
  metaspace/overhead/managed components explicitly shrunk in
  `flinkConfiguration` so those small sizes are valid); the Connect worker and
  Schema Registry JVMs are bounded via `KAFKA_HEAP_OPTS` /
  `SCHEMA_REGISTRY_HEAP_OPTS`.

## Quick start

```bash
scripts/minikube-up.sh
```

That will, in order:

1. `minikube start` (sized for the workload), if not already running.
2. Build **two images inside minikube's docker** (no local JDK/Maven needed):
   `s3-table-dump:dev` (Flink 2.0 + maintenance jar in `/opt/flink/usrlib`)
   and `s3-table-dump-connect:dev` (`cp-kafka-connect` + the Iceberg sink
   plugin + JSON Schema converter, from `Dockerfile.connect`).
3. Install the **Flink Kubernetes Operator** (Helm, `webhook.create=false` to
   skip cert-manager; `watchNamespaces` makes it create the `flink` RBAC).
4. Deploy infra: `postgres`, `minio` (+ a `bucket-setup` sidecar that creates
   the `warehouse` bucket), `kafka` (KRaft, auto-creates `events` with 3
   partitions).
5. Deploy **ingest**: `schema-registry`, then `kafka-connect` (the Connect
   worker), then the `connector-setup` Job that registers the Iceberg sink
   connector, then `datagen` (the JSON Schema producer).
6. Apply the two maintenance `FlinkDeployment`s — `maintenance`
   (rewrite+expire) and `orphan-gc` — both starting **suspended** — and
   [`k8s/maintenance-cron.yaml`](k8s/maintenance-cron.yaml): RBAC + two
   `CronJob`s that wake them on their schedules.

First run downloads the minikube base image, Maven dependencies, and the
Confluent images, so give it several minutes.

## Verify it works

**Pods** — ingest (Kafka Connect + Schema Registry) runs continuously;
`maintenance` and `orphan-gc` have pods only while a cron-triggered wake is in
progress:

```bash
kubectl -n s3-table-dump get pods         # kafka-connect/schema-registry always; maintenance/orphan-gc only during a wake
kubectl -n s3-table-dump get flinkdeployment   # maintenance + orphan-gc; SUSPENDED between wakes is the steady state
kubectl -n s3-table-dump get cronjob
```

**Ingest (Kafka Connect)** — the connector should be `RUNNING`:

```bash
kubectl -n s3-table-dump port-forward svc/kafka-connect 8083:8083 &
curl -s localhost:8083/connectors/iceberg-sink/status | jq      # state: RUNNING (connector + task)
kubectl -n s3-table-dump logs deploy/datagen -f                 # watch v1 -> v2 schema evolution
```

**Force a maintenance wake now** instead of waiting for cron:

```bash
kubectl -n s3-table-dump create job --from=cronjob/maintenance-runner maintenance-now
kubectl -n s3-table-dump create job --from=cronjob/orphan-gc-runner orphan-gc-now
```

Each creates a one-off pod that flips the target `FlinkDeployment` to
`running`, sleeps the wake window, and flips it back to `suspended`.

**Web UIs / REST** (maintenance/orphan-gc Flink UIs only respond mid-wake):

```bash
kubectl -n s3-table-dump port-forward svc/schema-registry 8081:8081  # GET /subjects, /subjects/events-value/versions
kubectl -n s3-table-dump port-forward svc/maintenance-rest 8082:8081 # rewrite+expire (mid-wake only)
kubectl -n s3-table-dump port-forward svc/orphan-gc-rest 8084:8081   # orphan GC (mid-wake only)
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

**Catalog & lock in Postgres** — `iceberg_tables` should list `db.events`
(created by the connector, not by any Flink job):

```bash
kubectl -n s3-table-dump exec deploy/postgres -- \
  psql -U iceberg -d iceberg -c "\dt" -c "select * from iceberg_tables;"
```

## Schema mapping & evolution

This is the point of running ingest on Kafka Connect: the **Iceberg table
schema comes from the JSON Schema in Schema Registry**, not from any code.

**Mapping.** `scripts/datagen.sh` produces records with the Confluent JSON
Schema serializer, which registers the schema under subject `events-value`.
The connector's `JsonSchemaConverter` turns each record into a Connect struct,
and the Iceberg sink maps Connect types → Iceberg types when it auto-creates
`db.events`:

| JSON Schema field | Connect type | Iceberg column |
|---|---|---|
| `id` / `event_type` / `user_id` — `{"type":"string"}` | STRING | `string` |
| `amount` — `{"type":"number"}` | FLOAT64 | `double` |
| `event_time` — number tagged as the Timestamp logical type | Timestamp (logical) | `timestamptz` |

`event_time` is the one subtlety: JSON Schema has no native timestamp, so it is
declared as the Kafka Connect **Timestamp logical type** and its wire value is
**epoch milliseconds** (a number), which the connector converts to
`timestamptz` (`SchemaUtils` → `TimestampType.withZone()`).

**Evolution.** `datagen` runs in two phases (`PHASE1_COUNT` controls the
switch). Phase 1 uses **v1** (`id, event_type, user_id, amount, event_time`);
phase 2 registers **v2**, which *adds an optional* `currency` field. Both
schemas use a **closed content model** (`"additionalProperties": false`) — a
JSON-Schema subtlety that matters: adding a property to an *open* model is
*not* BACKWARD-compatible (Schema Registry rejects it as
`PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL`, since old data could already carry a
differently-typed field), whereas adding an optional property to a *closed*
model is. So Schema Registry accepts v2 as version 2, and because the connector
runs with `iceberg.tables.evolve-schema-enabled` it **ALTERs the table to add
the `currency` column live**, no redeploy. Watch it happen:

```bash
# 1) two schema versions registered:
curl -s localhost:8081/subjects/events-value/versions          # [1, 2]  (schema-registry port-forward)
# 2) the new column appears in the Iceberg table:
kubectl -n s3-table-dump exec deploy/postgres -- \
  psql -U iceberg -d iceberg -c "select * from iceberg_tables;" # metadata_location advances
# 3) in the datagen log, the "evolving schema: registering v2" line marks the switch:
kubectl -n s3-table-dump logs deploy/datagen | grep -i evolv
```

A change that is *not* registry-compatible (a required new field, a type
change, a rename, or adding a field to an open content model) is rejected at
registration; the datagen producer then exits non-zero and the pod
crash-loops, and the table does not evolve — correct registry behaviour, not a
bug.

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
  a 240 s wake, ~60 s elapses before the first orphan pass; a second pass
  may run before suspend (idempotent, so harmless).
* Same `IcebergCatalog.lockFactory()` (lock id `db.events`) as the maintenance
  job → the Postgres lock serialises it against rewrite/expire when wake
  windows overlap.

Cron schedule + wake duration live in
[`k8s/maintenance-cron.yaml`](k8s/maintenance-cron.yaml):

* `schedule:` — `*/30 * * * *` for rewrite/expire, `0 */6 * * *` for orphan GC.
* The `sleep` value inside each CronJob's command is the wake window
  (default 240 s). Bump it if your minikube is slow enough that JM+TM
  cold-start eats too much of the window before the in-job triggers tick.
* `concurrencyPolicy: Forbid` — overlapping wakes against the same
  FlinkDeployment can't happen.

Ingest behaviour is **connector config** in
[`k8s/connector-setup.yaml`](k8s/connector-setup.yaml) — commit cadence
(`iceberg.control.commit.interval-ms`, default 60 s here), partitioning
(`iceberg.tables.default-partition-by`), and the `auto-create` / `evolve-schema`
flags — plus the event shape, JSON Schemas and phase-1 count in
[`scripts/datagen.sh`](scripts/datagen.sh). The Flink maintenance jobs read
connection settings from environment variables with defaults matching the
in-cluster Service names (`kafka`, `postgres`, `minio`) — see
[`IcebergCatalog.fromEnv()`](src/main/java/com/example/flinkiceberg/IcebergCatalog.java),
so only `S3_REGION` is set explicitly in the `FlinkDeployment`s; those same
hosts appear as `iceberg.catalog.*` in the connector config. Per-cluster
CPU/memory live in [`k8s/flink-maintenance.yaml`](k8s/flink-maintenance.yaml) /
[`k8s/flink-orphan-gc.yaml`](k8s/flink-orphan-gc.yaml); the Connect worker's in
[`k8s/kafka-connect.yaml`](k8s/kafka-connect.yaml).

## Build / run locally (without Kubernetes)

The jar holds the **Flink maintenance** jobs only (ingest is the Kafka Connect
connector, run via the worker image):

```bash
./mvnw -DskipTests package          # produces target/app.jar (0.0.0-SNAPSHOT)

# maintenance job (jar manifest main class):
flink run target/app.jar
# orphan-gc job (override the main class):
flink run -c com.example.flinkiceberg.IcebergOrphanGcJob target/app.jar
```

To stamp a real version, pass `-Drevision` from git-semver-release — see
[Versioning](#versioning). Override connection defaults via env vars, e.g.
`ICEBERG_JDBC_URI=jdbc:postgresql://localhost:5432/iceberg`.

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

* **All images/charts are pinned** for reproducibility (`k8s/*`, `Dockerfile`,
  `Dockerfile.connect`, `OPERATOR_VERSION` in `scripts/minikube-up.sh`). The
  Confluent images + JSON Schema converter are pinned to CP 8.2.1 (Confluent
  Community License). MinIO archived its own Docker Hub repo in 2025, so
  everything MinIO-related uses the maintained `alpine/minio` rebuild (runs as
  root via `runAsUser: 0` to write the `emptyDir`). No `mc` image: a
  `bucket-setup` **sidecar** in the minio pod shares the data volume and
  `mkdir`s `/data/warehouse` once minio has formatted the drive (a top-level
  dir *is* a bucket in MinIO's single-drive backend).
* The **table is created by the connector**, not by Flink — with
  `auto-create-enabled` the sink creates the `db` namespace
  (`IcebergWriterFactory.createNamespaceIfNotExist`) and the `db.events` table
  on its first batch, and `evolve-schema-enabled` ALTERs it thereafter. In
  **Application Mode** each maintenance job's `main()` runs in its
  **JobManager pod**, so `IcebergCatalog.awaitJdbc()` runs there; the jobs just
  `tableLoader` the table. A maintenance wake that fires before the connector's
  first commit fails to load the table and simply retries on the next cron
  wake.
* **Resource isolation + scheduled maintenance:** each maintenance job is its
  own Application cluster with its own JM+TM pods and memory/CPU budget, fully
  separate from the Connect ingest worker — a maintenance OOM/crash can't
  affect ingest. The two maintenance clusters additionally ship suspended and
  only have pods during a cron-triggered wake, so steady-state RAM use is
  essentially ingest (Connect + Schema Registry) + infra. During an
  overlapping wake the footprint is still real, hence `--memory=7600`. If pods
  sit `Pending` during a wake, raise `MINIKUBE_MEMORY` or lower the
  `jobManager`/`taskManager` `resource` in the `FlinkDeployment`s.
* Iceberg's `CatalogLoader` requires `org.apache.hadoop.conf.Configuration` on
  the classpath even with `S3FileIO`; the shaded `hadoop-client-api/runtime`
  uber jars are bundled to satisfy that on **both** sides — the Flink jar
  (`pom.xml`) and the Connect plugin (`kafka-connect/pom.xml`).
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
cold start before its first trigger; orphan GC's `rateLimit(1 min)` is also
kept shorter than the wake window, so at least one orphan pass runs per wake.
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
well inside the 240 s wake window, so the lock is released before the cron
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

Validated end-to-end on minikube: both maintenance clusters reach
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
