# AGENTS.md

Guidance for AI agents working in this repository.

## What this is

**Kafka S3 Iceberg Dump** — a demo Apache Flink (Java) app that streams JSON
events from Kafka into an Apache Iceberg table on S3 (MinIO), with Iceberg
**table maintenance** running as separate jobs. **Three** independent Flink
Application-Mode clusters (Flink Kubernetes Operator): `ingest`
(`KafkaToIcebergJob`), `maintenance` (`IcebergMaintenanceJob` =
`RewriteDataFiles` + `ExpireSnapshots`) and `orphan-gc`
(`IcebergOrphanGcJob` = `DeleteOrphanFiles`). Each gets its own
JobManager+TaskManager (isolated memory/CPU + failure domain). `maintenance`
and `orphan-gc` share the **same** Postgres maintenance lock id
(`db.events`), so the lock serialises orphan GC against expire/rewrite. The
whole stack runs on **minikube** (`scripts/minikube-up.sh`).

## Build & run

```bash
./mvnw -DskipTests package          # local jar -> target/app.jar (optional)
scripts/minikube-up.sh              # full stack on minikube (idempotent)
scripts/minikube-down.sh            # delete the whole minikube profile
scripts/minikube-down.sh --keep-cluster   # only remove the app + operator
```

* No local JDK/Maven required for the minikube path — the multi-stage
  `Dockerfile` compiles the jar in a `maven:3.9.9-eclipse-temurin-21` stage,
  then bakes it into a `flink:2.0.2-java21` image at `/opt/flink/usrlib`. The
  image is built **inside minikube's docker** (no registry); FlinkDeployments
  use `imagePullPolicy: Never`.
* Local builds use Java 21 (`maven.compiler.release=21`).
* There are **no automated tests**; verify by running the stack (see README
  "Verify it works"): `kubectl -n s3-table-dump get flinkdeployment,pods`,
  port-forward `svc/ingest-rest` / `svc/maintenance-rest` (separate Flink UIs
  — one per cluster), MinIO console (`svc/minio` :9001, admin/password), and
  the maintenance TaskManager pod logs.

## Layout

| Path | Purpose |
|---|---|
| `src/main/java/.../IcebergCatalog.java` | shared: `SCHEMA`/`SPEC`, env-driven catalog config, idempotent `ensureTable`, `JdbcLockFactory` |
| `src/main/java/.../KafkaToIcebergJob.java` | ingest job (jar manifest main class): Kafka source → `IcebergSink` |
| `src/main/java/.../IcebergMaintenanceJob.java` | maintenance job: `RewriteDataFiles` + `ExpireSnapshots` |
| `src/main/java/.../IcebergOrphanGcJob.java` | orphan-GC job: `DeleteOrphanFiles` only, shares the `db.events` lock id |
| `src/main/java/.../JsonToRowData.java` | JSON → Iceberg `RowData` mapper |
| `src/main/java/.../RetryingTriggerLockFactory.java` | retries `TriggerLockFactory.open()` on the TM (absorbs JDBC cold-connect race) |
| `pom.xml` | deps + `maven-shade-plugin` (fat jar, `ServicesResourceTransformer`) |
| `Dockerfile` | multi-stage: Maven build → Flink 2.0 image with jar in `usrlib` |
| `k8s/` | `namespace`, `postgres`, `minio`, `kafka`, `datagen`, and the three `FlinkDeployment`s (`flink-ingest.yaml`, `flink-maintenance.yaml`, `flink-orphan-gc.yaml`) |
| `scripts/minikube-up.sh` / `minikube-down.sh` | bring up / tear down on minikube (image build, operator, infra, apps) |
| `scripts/datagen.sh` | POSIX-sh JSON generator, piped into `kafka-console-producer` (no kcat) |

## Architecture invariants — do not break

* **Flink is pinned to the 2.0 line.** Iceberg ships a Flink connector only
  for Flink 2.0 (`iceberg-flink-runtime-2.0`). Do **not** bump `flink.version`
  past `2.0.x` unless a matching `iceberg-flink-runtime-<newer>` exists.
  Versions are cross-checked in `pom.xml` properties.
* **Three jobs, one image, three clusters.** `KafkaToIcebergJob` (ingest,
  the jar's manifest main class), `IcebergMaintenanceJob` (rewrite+expire) and
  `IcebergOrphanGcJob` (orphan GC) ship in the *same* image and run as
  *separate Flink Application-Mode clusters* (one `FlinkDeployment` each, the
  class set via `entryClass`). Shared schema/catalog/lock wiring lives in
  `IcebergCatalog`; keep it the single source of truth — don't duplicate
  catalog props or `SCHEMA` into the job classes. The split is **physical**:
  each job has its own JM+TM pods and memory/CPU budget (resource isolation —
  the point; don't merge them back into a session cluster).
* **`maintenance` and `orphan-gc` MUST share the same lock id.** Both call
  `IcebergCatalog.lockFactory()`, whose `JdbcLockFactory` lock id is
  `db.events`. That shared Postgres lock is exactly what serialises
  `DeleteOrphanFiles` against `ExpireSnapshots`'s deletes (the S3-404 fix).
  Do not give orphan-gc its own/different lock id, and don't fold orphan
  removal back into `IcebergMaintenanceJob`'s `TableMaintenance` graph —
  that reintroduces the in-graph delete race. Iceberg 1.10.2's Flink API
  *does* include `DeleteOrphanFiles`; don't claim orphan removal needs
  another engine.
* **Catalog = Iceberg JDBC catalog on Postgres**; the *same* Postgres backs the
  maintenance `JdbcLockFactory` (`IcebergCatalog.lockFactory()`), so the lock
  holds across both separately-deployed jobs. Storage = MinIO via `S3FileIO`
  (path-style). All connection settings are **environment-variable driven**
  with container-friendly defaults (see `IcebergCatalog.fromEnv`). Keep new
  config env-driven; don't hardcode hosts.
* **`JsonToRowData` field order must match `IcebergCatalog.SCHEMA`** exactly
  (id, event_type, user_id, amount, event_time). Changing the schema means
  changing both, in lockstep.
* **Checkpointing must stay enabled in both jobs** — the ingest Iceberg sink
  commits on checkpoint (without it nothing is written); the maintenance job's
  `TableMaintenance` source checkpoints its processed-snapshot state.
* `hadoop-client-api/runtime` are bundled deliberately: Iceberg's
  `CatalogLoader` needs `org.apache.hadoop.conf.Configuration` on the classpath
  even with `S3FileIO`. Don't remove them.
* **Application Mode** runs each job's `main()` in its **JobManager pod** (no
  separate submitter/client), so `IcebergCatalog.awaitJdbc()` /
  `ensureTable()` run there and the JM pod needs network access to Postgres +
  MinIO (in-cluster Services `postgres`/`minio`). `ensureTable()` is
  idempotent and swallows `AlreadyExistsException`, so the two clusters can
  come up in any order — there is no submit-ordering dependency to preserve.

## Gotchas

* Two Flink clusters need real RAM. `scripts/minikube-up.sh` starts minikube
  with `--memory=7600` (override `MINIKUBE_MEMORY`). Per-cluster JM/TM
  `resource` is tuned lean in `k8s/flink-*.yaml` (JM 768m / TM 1280m, 1 slot).
  If TM pods sit `Pending`, raise `MINIKUBE_MEMORY` or lower those — don't
  collapse the two clusters back into one to save memory (that's the
  limitation this design removes).
* Maintenance-startup JDBC cold-connect race (`JdbcLockFactory.open()` →
  `UncheckedSQLException`, thrown on the **TaskManager** operator). Three
  layers, keep all three — they do different jobs:
  (1) **`RetryingTriggerLockFactory`** wraps the JDBC lock and retries
  `open()` in-place on the TM (5 × 3 s) — this is what actually eliminates the
  restart; don't unwrap it.
  (2) `IcebergCatalog.awaitJdbc()` is only a *fail-fast* pre-flight in the
  JobManager pod (clear error if Postgres is truly down); it cannot prevent
  the TM race — don't "upgrade" it expecting it to.
  (3) `restart-strategy.type: fixed-delay` (3 / 5 s), set in each
  `FlinkDeployment.spec.flinkConfiguration`, is the bounded last resort.
  Expected: `numRestarts=0`, empty exception history. A growing history /
  restart loop is the real signal — don't "fix" it by removing the JDBC lock.
* `DeleteOrphanFiles` HEADs files (`BaseS3File.getObjectMetadata`); a
  *concurrent* `ExpireSnapshots` delete makes that HEAD 404
  (`NoSuchKeyException`) → `TaskResult{success=false}`. Architectural fix:
  isolate orphan GC into `IcebergOrphanGcJob`/`orphan-gc` taking the **same**
  `db.events` maintenance lock as `IcebergMaintenanceJob` (validated: orphan-gc
  TM logs `Created/Deleted JdbcLock{lockId=db.events}`; keep that shared lock
  id — see invariant). Caveat: `ExpireSnapshots`' bulk-delete tail can run
  past lock release, so the 404 is still observed in practice — an
  Iceberg-operator-level race we can't fix from outside the operator.
  **Contained**: job RUNNING, `numRestarts=0`, no exception entry, only that
  orphan pass is `success=false`, retries next 15-min interval. Don't "fix" by
  hacking the driver/lock or by deleting orphan removal.
* Re-applying a `FlinkDeployment` makes the operator redeploy *that one*
  cluster; for a clean run use `scripts/minikube-down.sh` then
  `scripts/minikube-up.sh`.
* All image tags, the Flink Kubernetes Operator version (`OPERATOR_VERSION`
  in `scripts/minikube-up.sh`) **and** `pom.xml` deps/plugins are pinned —
  keep them pinned (no `latest`/floating tags). MinIO archived its own Docker
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
* Tuning knobs live in code, not SQL: rewrite/expire in
  `IcebergMaintenanceJob`, orphan GC in `IcebergOrphanGcJob`, ingest (Kafka
  source, checkpoint interval) in `KafkaToIcebergJob`.
  `DeleteOrphanFiles.minAge` must stay comfortably above the ingest commit
  cadence or in-flight files get deleted.
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
