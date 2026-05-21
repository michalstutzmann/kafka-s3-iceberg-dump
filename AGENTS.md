# AGENTS.md

Guidance for AI agents working in this repository.

## What this is

**Kafka S3 Iceberg Dump** — a demo that streams JSON events from Kafka into an
Apache Iceberg table on S3 (MinIO), with Iceberg **table maintenance** running
as separate Flink jobs. Two runtimes, deliberately split:

* **Ingest = Apache Iceberg Kafka Connect sink connector** (not Flink). A
  single Kafka Connect worker (`k8s/kafka-connect.yaml`) runs the
  `org.apache.iceberg.connect.IcebergSinkConnector`, registered by the
  `connector-setup` Job (`k8s/connector-setup.yaml`). Records are produced as
  **JSON Schema** through **Confluent Schema Registry**
  (`k8s/schema-registry.yaml`); the connector's `JsonSchemaConverter` maps the
  registry schema to Iceberg types, and **owns the table** —
  `auto-create-enabled` + `evolve-schema-enabled` mean it creates `db.events`
  and ALTERs it as the schema evolves. There is **no hardcoded Iceberg schema
  in Java** anymore.
* **Maintenance = two Flink Application-Mode clusters** (Flink Kubernetes
  Operator): `maintenance` (`IcebergMaintenanceJob` = `RewriteDataFiles` +
  `ExpireSnapshots`) and `orphan-gc` (`IcebergOrphanGcJob` =
  `DeleteOrphanFiles`). Each gets its own JobManager+TaskManager (isolated
  memory/CPU + failure domain). They share the **same** Postgres maintenance
  lock id (`db.events`), so the lock serialises orphan GC against
  expire/rewrite. Both ship SUSPENDED and are woken on a schedule by
  Kubernetes `CronJob`s (see [`k8s/maintenance-cron.yaml`](k8s/maintenance-cron.yaml))
  — outside their wake windows they have zero JM/TM pods.

Ingest (Kafka Connect) runs continuously. The Flink jobs and the Kafka Connect
ingest **share one Iceberg JDBC-catalog-on-Postgres table** (`db.events`) on
MinIO. The whole stack runs on **minikube** (`scripts/minikube-up.sh`).

## Build & run

```bash
./mvnw -DskipTests package          # local jar -> target/app.jar (optional)
scripts/minikube-up.sh              # full stack on minikube (idempotent)
scripts/minikube-down.sh            # delete the whole minikube profile
scripts/minikube-down.sh --keep-cluster   # only remove the app + operator
```

* No local JDK/Maven required for the minikube path. **Two images** are built
  **inside minikube's docker** (no registry; consumers use
  `imagePullPolicy: Never`):
  - `Dockerfile` → `s3-table-dump:dev`: multi-stage, compiles the Flink
    maintenance jar in a `maven:3.9.9-eclipse-temurin-21` stage, then bakes it
    into a `flink:2.0.2-java21` image at `/opt/flink/usrlib`.
  - `Dockerfile.connect` → `s3-table-dump-connect:dev`: assembles the Iceberg
    Kafka Connect plugin (via `kafka-connect/pom.xml`, `dependency:copy-dependencies`)
    onto `confluentinc/cp-kafka-connect`, plus the Confluent JSON Schema
    converter.
* Local builds use Java 21 (`maven.compiler.release=21`).
* There are **no automated tests**; verify by running the stack (see README
  "Verify it works"): `kubectl -n s3-table-dump get flinkdeployment,pods`,
  the connector status (`curl svc/kafka-connect:8083/connectors/iceberg-sink/status`),
  the registered schemas (`svc/schema-registry` :8081 `/subjects`),
  `svc/maintenance-rest` (Flink UI, mid-wake only), MinIO console
  (`svc/minio` :9001, admin/password), and the maintenance TaskManager logs.

## Layout

| Path | Purpose |
|---|---|
| `src/main/java/.../IcebergCatalog.java` | shared (maintenance jobs): env-driven catalog config + `JdbcLockFactory`. **No** `SCHEMA`/`ensureTable` — the connector owns the table |
| `src/main/java/.../IcebergMaintenanceJob.java` | maintenance job (jar manifest main class): `RewriteDataFiles` + `ExpireSnapshots` |
| `src/main/java/.../IcebergOrphanGcJob.java` | orphan-GC job: `DeleteOrphanFiles` only, shares the `db.events` lock id |
| `src/main/java/.../RetryingTriggerLockFactory.java` | retries `TriggerLockFactory.open()` on the TM (absorbs JDBC cold-connect race) |
| `pom.xml` | Flink maintenance jar: deps + `maven-shade-plugin` (fat jar, `ServicesResourceTransformer`) |
| `Dockerfile` | multi-stage: Maven build → Flink 2.0 image with jar in `usrlib` |
| `kafka-connect/pom.xml` | build-only: gathers the Iceberg connector + AWS/Postgres/Hadoop deps into a Connect plugin dir (`dependency:copy-dependencies`) |
| `Dockerfile.connect` | `cp-kafka-connect` + the assembled Iceberg plugin + Confluent JSON Schema converter |
| `k8s/` | `namespace`, `postgres`, `minio`, `kafka`, `schema-registry`, `kafka-connect` (ingest worker), `connector-setup` (registers the sink connector), `datagen`, the two maintenance `FlinkDeployment`s (`flink-maintenance.yaml`, `flink-orphan-gc.yaml`), and `maintenance-cron.yaml` (RBAC + two `CronJob`s that wake the suspended clusters) |
| `scripts/minikube-up.sh` / `minikube-down.sh` | bring up / tear down on minikube (build both images, operator, infra, ingest, maintenance) |
| `scripts/datagen.sh` | bash JSON-Schema generator via `kafka-json-schema-console-producer`; phased v1→v2 to demo schema evolution |

## Architecture invariants — do not break

* **Flink (maintenance) is pinned to the 2.0 line; the connector to Iceberg
  1.10.2.** Iceberg ships a Flink connector only for Flink 2.0
  (`iceberg-flink-runtime-2.0`). Do **not** bump `flink.version` past `2.0.x`
  unless a matching `iceberg-flink-runtime-<newer>` exists. The Kafka Connect
  plugin (`kafka-connect/pom.xml`) uses `iceberg-kafka-connect` at the **same**
  `iceberg.version` (1.10.2) as the Flink jobs — keep the two `iceberg.version`
  properties in lockstep so ingest and maintenance run against one Iceberg
  line. Versions are cross-checked in `pom.xml` / `kafka-connect/pom.xml`.
* **Ingest is Kafka Connect; maintenance is two Flink clusters.** The two Flink
  jobs — `IcebergMaintenanceJob` (rewrite+expire, the jar's manifest main
  class) and `IcebergOrphanGcJob` (orphan GC) — ship in the *same* image and
  run as *separate Flink Application-Mode clusters* (one `FlinkDeployment`
  each, class via `entryClass`); shared catalog/lock wiring lives in
  `IcebergCatalog` (single source of truth — don't duplicate catalog props
  into the job classes). The split is **physical**: own JM+TM pods and
  memory/CPU budget. **Ingest does not run on Flink** — don't reintroduce a
  Flink ingest job; it lives in the Kafka Connect sink connector (configured
  in `k8s/connector-setup.yaml`).
* **The connector owns the table schema — don't put it back in Java.** The
  sink runs with `iceberg.tables.auto-create-enabled` +
  `iceberg.tables.evolve-schema-enabled`, so it creates `db.events` (and the
  `db` namespace — `IcebergWriterFactory.createNamespaceIfNotExist`) and ALTERs
  it from the registered JSON Schema. There is intentionally **no** Java
  `SCHEMA`/`SPEC`/`ensureTable` and **no** JSON→RowData mapper. Partitioning
  (`identity(event_type)`) is set via `iceberg.tables.default-partition-by`.
  Don't reintroduce a hardcoded schema or a lockstep mapper "to be safe" — it
  defeats the whole point of registry-driven mapping/evolution.
* **Maintenance clusters are cron-driven, not continuous.** `maintenance`
  and `orphan-gc` ship with `spec.job.state: suspended` and
  `upgradeMode: stateless`. The CronJobs in `k8s/maintenance-cron.yaml`
  (ServiceAccount `maintenance-runner` with `get`+`patch` on
  `flinkdeployments`, image `alpine/k8s` — distroless `rancher/kubectl` has
  no `/bin/sh` and the script needs a shell) flip `spec.job.state` to
  `running`, sleep for the wake window, then flip back to `suspended`. The
  cron is **the** schedule; the in-job triggers just guarantee at least one
  pass fires inside the wake window. Iceberg's gating on a stateless cold
  start, **measured empirically here**:
  - `scheduleOnCommitCount(N)` fires on the first source tick if the table
    has ≥N commits in history (validated: rewrite fired ~3 s after
    `TriggerManager` init).
  - `scheduleOnInterval(D)` together with `TableMaintenance.rateLimit(R)`
    fires after **max(D, R)** wall-clock from job start — both
    `lastTriggerTime` and the rate limiter's `lastFireTime` are
    initialised to job-start (validated: with `D=20s`, `R=5min`, first
    orphan fire was 5 min 1 s after init — confirming the gate). Keep
    `max(D, R)` comfortably shorter than the cron wake window or the pass
    never runs. Current values: rewrite/expire `rateLimit(1min)` +
    commit-count triggers; orphan-gc `rateLimit(1min)` + `interval(20s)`.
* **`maintenance` and `orphan-gc` MUST share the same lock id.** Both call
  `IcebergCatalog.lockFactory()`, whose `JdbcLockFactory` lock id is
  `db.events`. That shared Postgres lock is exactly what serialises
  `DeleteOrphanFiles` against `ExpireSnapshots`'s deletes (the S3-404 fix).
  Do not give orphan-gc its own/different lock id, and don't fold orphan
  removal back into `IcebergMaintenanceJob`'s `TableMaintenance` graph —
  that reintroduces the in-graph delete race. Iceberg 1.10.2's Flink API
  *does* include `DeleteOrphanFiles`; don't claim orphan removal needs
  another engine.
* **One catalog, two writers.** Catalog = Iceberg **JDBC catalog on Postgres**;
  storage = MinIO via `S3FileIO` (path-style). The Kafka Connect sink's
  `iceberg.catalog.*` properties (`k8s/connector-setup.yaml`) and the Flink
  jobs' `IcebergCatalog.fromEnv()` must describe the **same** catalog/warehouse
  so both write `db.events`. The *same* Postgres also backs the maintenance
  `JdbcLockFactory` (`IcebergCatalog.lockFactory()`). All Flink-side connection
  settings are **environment-variable driven** with container-friendly
  defaults; keep them env-driven and keep them matching the connector config.
* **Schema mapping flows registry → connector → table; `event_time` is the
  one subtlety.** Field types come from the JSON Schema datagen registers:
  string→string, `{"type":"number"}`→double. `event_time` is declared as the
  **Kafka Connect `Timestamp` logical type** in JSON Schema
  (`{"type":"number","title":"org.apache.kafka.connect.data.Timestamp","connect.type":"int64"}`),
  which the connector maps to Iceberg **`timestamptz`** (`SchemaUtils` →
  `TimestampType.withZone()`). Because of that, its on-the-wire value must be
  **epoch milliseconds (a number)**, not an ISO string — see
  `scripts/datagen.sh`. Schema *evolution* requires the new JSON Schema to be
  registry-compatible (default BACKWARD), and two non-obvious JSON-Schema rules
  the demo schemas depend on (both **verified** against the running registry):
  (1) both schemas use a **closed content model** (`"additionalProperties":
  false`) — adding a property to an *open* model is rejected as
  `PROPERTY_ADDED_TO_OPEN_CONTENT_MODEL` (old data could already carry a
  differently-typed field), so the v2 add validates *only* because the model is
  closed; (2) the new `currency` field is **optional** (not in `required`).
  Change either and registration fails and the table won't evolve.
* **Checkpointing must stay enabled in both Flink maintenance jobs** — their
  `TableMaintenance` source checkpoints its processed-snapshot cursor. With
  cron-driven wakes + `upgradeMode: stateless` that cursor is deliberately
  discarded between wakes (each wake is a cold start — that's why we use
  commit-count over the table's all-time history rather than
  since-job-start), but checkpointing still has to be on for the source to
  operate during a wake. (Ingest commit cadence is the connector's
  `iceberg.control.commit.interval-ms`, not a Flink checkpoint.)
* `hadoop-client-api/runtime` are bundled deliberately **on both sides** (Flink
  jar *and* the Connect plugin in `kafka-connect/pom.xml`): Iceberg's catalog
  loading needs `org.apache.hadoop.conf.Configuration` on the classpath even
  with `S3FileIO`. Don't remove them. Likewise the Postgres JDBC driver is not
  in the connector distribution — `kafka-connect/pom.xml` adds it.
* **Application Mode** runs each Flink job's `main()` in its **JobManager pod**
  (no separate submitter/client), so `IcebergCatalog.awaitJdbc()` runs there
  and the JM pod needs network access to Postgres + MinIO. The maintenance jobs
  **no longer create the table** (the connector does) — they just `tableLoader`
  it. A maintenance wake that fires before the connector's first commit fails
  to load the table and retries on the next cron wake; by then ingest has
  created it. No submit-ordering dependency to preserve.

## Gotchas

* Ingest (the `kafka-connect` Deployment) runs continuously. `maintenance` and
  `orphan-gc` Flink pods exist only inside a cron-triggered wake window
  (default 240 s; `sleep` value in `k8s/maintenance-cron.yaml`). Outside that
  window `kubectl get pods` will show no JM/TM for those two clusters — that's
  the point, not a failure. To verify wake plumbing without waiting for cron,
  force a run: `kubectl -n s3-table-dump create job --from=cronjob/maintenance-runner maintenance-now`.
* **CP images need `enableServiceLinks: false`** (set on the schema-registry
  pod). A Service named `schema-registry` makes Kubernetes inject service-link
  env vars (`SCHEMA_REGISTRY_PORT=tcp://<clusterIP>:8081`, …) into pods; CP's
  entrypoint reads every `SCHEMA_REGISTRY_*` var as config, parses the
  `tcp://…` value as the deprecated `port`, and the container exits 1 on
  startup (CrashLoopBackOff, logs stop right after "Configuring …"). Verified
  fix; don't remove it. (kafka-connect reads `CONNECT_*`, not the injected
  vars, so it's unaffected.)
* **The connector plugin must include `iceberg-aws` AND `iceberg-parquet`**
  (see `kafka-connect/pom.xml`). `iceberg-kafka-connect` does not pull them
  transitively — the Flink runtime *uber jar* shades them in, but the manually
  assembled Connect plugin doesn't, so the task fails at runtime with
  `ClassNotFoundException: org.apache.iceberg.aws.s3.S3FileIO` (FileIO) or
  `NoClassDefFoundError: org/apache/iceberg/parquet/Parquet` (default write
  format). `iceberg-aws-bundle` ships only the AWS SDK, not the
  `org.apache.iceberg.aws.*` classes — both deps are needed.
* The connector is registered by the `connector-setup` **Job**, not by the
  worker itself. If ingest isn't writing, check that order: worker
  `Running` → Job completed → `curl kafka-connect:8083/connectors/iceberg-sink/status`
  shows `RUNNING`. The Job waits for both Connect and Schema Registry REST
  before `PUT`ting, and the `PUT …/config` form is idempotent (re-run safe).
  `scripts/minikube-up.sh` deletes+reapplies the Job each run because Job specs
  are largely immutable.
* The control topic (`control-iceberg`) and the Connect internal topics rely on
  the broker's `auto.create.topics.enable` (apache/kafka default = true) and
  the `CONNECT_*_REPLICATION_FACTOR: "1"` env on the single-broker demo. The
  sink uses exactly-once (KIP-447), fine on Kafka 4.0.
* Schema **evolution won't happen** if the v2 schema isn't registry-compatible
  (default BACKWARD). For JSON Schema specifically that means the schemas must
  be a **closed content model** and the added field **optional** (see the
  schema-mapping invariant above). A required new field, a type change, a
  rename, or adding a field to an *open* model is rejected at registration
  (datagen logs the producer error, exits non-zero, and the Deployment
  crash-loops re-running phase 1) and the table won't evolve. Correct registry
  behaviour, not a bug — but it does take the datagen pod down, so a crash-
  looping datagen is the signal the v2 schema isn't compatible.
* The cluster still needs real RAM during a wake — and a wake can overlap
  with ingest. `scripts/minikube-up.sh` starts minikube with `--memory=7600`
  (override `MINIKUBE_MEMORY`). Per-cluster JM/TM `resource` is tuned lean
  in `k8s/flink-*.yaml`. If TM pods sit `Pending` during a wake, raise
  `MINIKUBE_MEMORY` or lower those — don't switch off the maintenance
  clusters' suspended-by-default model to "save memory" (it's what frees
  RAM in steady state).
* Maintenance-startup JDBC cold-connect race (`JdbcLockFactory.open()` →
  `UncheckedSQLException`, thrown on the **TaskManager** operator). Now
  matters on **every cron-triggered wake**, not just on first deploy —
  stateless wakes always cold-start the JDBC pool. Three layers, keep all
  three — they do different jobs:
  (1) **`RetryingTriggerLockFactory`** wraps the JDBC lock and retries
  `open()` in-place on the TM (5 × 3 s) — this is what actually eliminates
  the restart; don't unwrap it.
  (2) `IcebergCatalog.awaitJdbc()` is only a *fail-fast* pre-flight in the
  JobManager pod (clear error if Postgres is truly down); it cannot prevent
  the TM race — don't "upgrade" it expecting it to.
  (3) `restart-strategy.type: fixed-delay` (3 / 5 s), set in each
  `FlinkDeployment.spec.flinkConfiguration`, is the bounded last resort.
  Expected per-wake: `numRestarts=0`, empty exception history. A growing
  history / restart loop is the real signal — don't "fix" it by removing
  the JDBC lock.
* `DeleteOrphanFiles` HEADs files (`BaseS3File.getObjectMetadata`); a
  *concurrent* `ExpireSnapshots` delete makes that HEAD 404
  (`NoSuchKeyException`) → `TaskResult{success=false}`. Two layers of
  defence: (a) the shared `db.events` lock serialises within an overlapping
  wake (see invariant), and (b) the two CronJobs use disjoint schedules so
  overlap is rare in the first place. Even so, `ExpireSnapshots`' bulk-
  delete tail can run past lock release, so the 404 is still observed in
  practice — an Iceberg-operator-level race we can't fix from outside the
  operator. **Contained**: job RUNNING, `numRestarts=0`, no exception
  entry, only that orphan pass is `success=false`, retries next cron wake.
  Don't "fix" by hacking the driver/lock or by deleting orphan removal.
* Iceberg's `JdbcLockFactory` (table `flink_maintenance_lock`) has **no
  expiry / no heartbeat** — verified in
  `iceberg-flink-1.20:1.10.2`'s source: schema is
  `(LOCK_TYPE, LOCK_ID, INSTANCE_ID)`, primary key on the first two
  columns, no timestamp column. A SIGKILL'd TaskManager that was holding
  the lock leaves a row that blocks *all* future maintenance until cleared
  manually. With cron-driven wakes this matters: if a maintenance task
  hasn't released the lock by the time the CronJob's tail patches
  `state: suspended`, the operator kills the TM mid-hold and we lose the
  lock. In practice tasks finish in seconds against the small demo table,
  well inside the 240 s wake window, so it doesn't bite — but don't
  shorten the wake window below the slowest task's duration. Manual clear:
  `psql -c "DELETE FROM flink_maintenance_lock WHERE LOCK_ID='db.events';"`.
* Re-applying a `FlinkDeployment` makes the operator redeploy *that one*
  cluster; for a clean run use `scripts/minikube-down.sh` then
  `scripts/minikube-up.sh`.
* All image tags, the Flink Kubernetes Operator version (`OPERATOR_VERSION`
  in `scripts/minikube-up.sh`) **and** `pom.xml` / `kafka-connect/pom.xml`
  deps/plugins are pinned — keep them pinned (no `latest`/floating tags). The
  Confluent images (`cp-schema-registry`, `cp-kafka-connect`) and the
  `confluent-hub install …:8.2.1` converter are pinned to **CP 8.2.1** (Kafka
  4.x line, matches the apache/kafka 4.0 broker); bump the image tags and the
  converter version **together**. NOTE: the `cp-*` images are under the
  **Confluent Community License** (free, source-available, not OSI
  "open source") — a different license from the Apache-2.0 broker; acceptable
  for this demo. MinIO archived its own Docker
  Hub repo in 2025: everything MinIO uses the maintained `alpine/minio`
  rebuild (needs `runAsUser: 0` — it runs as uid 100 by default and can't
  write the `emptyDir`). `alpine/minio` has no `mc`; the **`bucket-setup`
  sidecar** in the minio pod shares the data volume and `mkdir`s
  `/data/warehouse` (a top-level dir = a bucket in MinIO's single-drive
  backend) after minio formats the drive (`/data/.minio.sys` appears). Don't
  reintroduce `minio/mc`, and keep bucket-setup a *sidecar* (emptyDir isn't
  shareable across pods, so it can't be a separate Job).

## Conventions

* Java: standard Flink/Iceberg DataStream API; keep operators named
  (`.name(...)`) for readability in the Flink UI.
* Tuning knobs: rewrite/expire in `IcebergMaintenanceJob`, orphan GC in
  `IcebergOrphanGcJob` (Java, not SQL). Ingest tuning is **connector config**
  in `k8s/connector-setup.yaml` (commit interval, partitioning, auto-create /
  evolve flags) and the producer in `scripts/datagen.sh` (event shape, the
  JSON Schemas, phase-1 count). Cron schedule + wake duration for the
  maintenance clusters live in `k8s/maintenance-cron.yaml` (`schedule:` + the
  `sleep` value). `DeleteOrphanFiles.minAge` must stay comfortably above the
  connector's commit cadence (`iceberg.control.commit.interval-ms`) or
  in-flight files get deleted.
* Keep README's stack table and version constraints in sync with `pom.xml`.
* Releases are tagged with
  [git-semver-release](https://github.com/michalstutzmann/git-semver-release)
  using **manual bumps** (`git-semver-release patch|minor|major`) — pick the
  SemVer level explicitly per release; we do **not** use Conventional Commits
  auto-bump, so don't rely on commit-message prefixes for versioning. This is a
  *project* version, independent of the pinned dependency versions above.
* The `pom.xml` version is a [Maven CI-friendly
  version](https://maven.apache.org/guides/mini/guide-maven-ci-friendly.html):
  `<version>${revision}</version>`, `revision` defaulting to `0.0.0-SNAPSHOT`.
  **Never hardcode `<version>` or edit the `revision` default to a real
  number** — git-semver-release is the single source of truth and feeds Maven
  via `-Drevision="$(git-semver-release version)"` (or the `APP_VERSION` env
  var, which `scripts/minikube-up.sh` passes as the Dockerfile `--build-arg`
  and the build stage forwards to `-Drevision`). The
  `flatten-maven-plugin` (`resolveCiFriendliesOnly`) resolves `${revision}` in
  the built POM and is required for ci-friendly versions on Maven 3 — don't
  remove it.
