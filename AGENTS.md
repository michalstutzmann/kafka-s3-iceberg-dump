# AGENTS.md

Guidance for AI agents working in this repository.

## What this is

**Kafka S3 Iceberg Dump** — a demo Apache Flink (Java) app that streams JSON
events from Kafka into an Apache Iceberg table on S3 (MinIO), with Iceberg
**table maintenance** (`RewriteDataFiles` + `ExpireSnapshots` +
`DeleteOrphanFiles`) running as a **separate Flink job**. Ingest
(`KafkaToIcebergJob`) and maintenance (`IcebergMaintenanceJob`) are deployed
as **two independent Flink Application-Mode clusters** via the **Flink
Kubernetes Operator** — each its own JobManager+TaskManager, so they have
isolated memory/CPU budgets and failure domains, with shared catalog/lock
wiring. The whole stack runs on **minikube** (`scripts/minikube-up.sh`).

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
| `src/main/java/.../IcebergMaintenanceJob.java` | maintenance job (run via `-c`): standalone self-triggering `TableMaintenance` |
| `src/main/java/.../JsonToRowData.java` | JSON → Iceberg `RowData` mapper |
| `src/main/java/.../RetryingTriggerLockFactory.java` | retries `TriggerLockFactory.open()` on the TM (absorbs JDBC cold-connect race) |
| `pom.xml` | deps + `maven-shade-plugin` (fat jar, `ServicesResourceTransformer`) |
| `Dockerfile` | multi-stage: Maven build → Flink 2.0 image with jar in `usrlib` |
| `k8s/` | `namespace`, `postgres`, `minio`, `kafka`, `datagen`, and the two `FlinkDeployment`s (`flink-ingest.yaml`, `flink-maintenance.yaml`) |
| `scripts/minikube-up.sh` / `minikube-down.sh` | bring up / tear down on minikube (image build, operator, infra, apps) |
| `scripts/datagen.sh` | POSIX-sh JSON generator, piped into `kafka-console-producer` (no kcat) |

## Architecture invariants — do not break

* **Flink is pinned to the 2.0 line.** Iceberg ships a Flink connector only
  for Flink 2.0 (`iceberg-flink-runtime-2.0`). Do **not** bump `flink.version`
  past `2.0.x` unless a matching `iceberg-flink-runtime-<newer>` exists.
  Versions are cross-checked in `pom.xml` properties.
* **Two jobs, one image, two clusters.** Ingest (`KafkaToIcebergJob`, the
  jar's manifest main class) and maintenance (`IcebergMaintenanceJob`, set via
  the `FlinkDeployment` `entryClass`) ship in the *same* image and run as
  *separate Flink Application-Mode clusters* (one `FlinkDeployment` each).
  Shared schema/catalog/lock wiring lives in `IcebergCatalog`; keep it the
  single source of truth — don't duplicate catalog props or `SCHEMA` into the
  job classes. The split is now **physical**: each job has its own JM+TM pods
  and memory/CPU budget (resource isolation — this is the point; don't merge
  them back into one session cluster). Maintenance is complete in-process:
  Iceberg 1.10.2's Flink maintenance API includes `DeleteOrphanFiles`, so the
  job does compaction + expiration + orphan-file removal — don't reintroduce a
  claim that orphan removal needs another engine (it does not in this version).
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
* `DeleteOrphanFiles` HEADs files (`BaseS3File.getObjectMetadata`) that a
  concurrent `ExpireSnapshots` may delete → transient S3 404
  (`NoSuchKeyException`), that orphan pass = `TaskResult{success=false}`. This
  is an **inherent** race of running orphan removal beside another deleter in
  one job, **contained** (job stays RUNNING, `numRestarts=0`, no exception
  entry, retries next interval) — it is **not** a bug to "fix" by hacking the
  driver/lock or removing orphan removal. It worked under the old single-JVM
  session run (timing-sensitive). Mitigated only by the long 30-min
  `scheduleOnInterval`. The real fix (don't do unless asked) is isolating
  orphan removal into its own infrequent job/window.
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
* Maintenance tuning knobs live in `IcebergMaintenanceJob` (the
  `RewriteDataFiles`/`ExpireSnapshots`/`DeleteOrphanFiles`/`TableMaintenance`
  builders) — adjust there, not via SQL. `DeleteOrphanFiles.minAge` must stay
  comfortably above the ingest commit cadence or in-flight files get deleted.
  Ingest knobs (Kafka source, checkpoint interval) live in `KafkaToIcebergJob`.
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
