# AGENTS.md

Guidance for AI agents working in this repository.

## What this is

**Kafka S3 Iceberg Dump** — a demo Apache Flink (Java) app that streams JSON
events from Kafka into an Apache Iceberg table on S3 (MinIO), with Iceberg
**table maintenance** (`RewriteDataFiles` + `ExpireSnapshots` +
`DeleteOrphanFiles`) running as a
**separate Flink job** built from the same jar. Two jobs (`kafka-to-iceberg`
ingest, `iceberg-maintenance`) deployed independently: independent lifecycles,
failure domains and tuning, shared catalog/lock wiring. The whole stack runs
via Docker Compose.

## Build & run

```bash
./mvnw -DskipTests package          # -> target/app.jar (shaded fat jar)
docker compose up --build           # full stack: build + infra + job + datagen
docker compose down -v              # tear down incl. volumes
```

* No local JDK/Maven required for the Compose path — the `builder` service
  compiles the jar in a `maven:3.9.9-eclipse-temurin-21` container.
* Local builds use Java 21 (`maven.compiler.release=21`).
* There are **no automated tests**; verify by running the stack (see README
  "Verify it works") and inspecting the Flink UI (`:8081`), MinIO console
  (`:9001`, admin/password), and TaskManager logs.

## Layout

| Path | Purpose |
|---|---|
| `src/main/java/.../IcebergCatalog.java` | shared: `SCHEMA`/`SPEC`, env-driven catalog config, idempotent `ensureTable`, `JdbcLockFactory` |
| `src/main/java/.../KafkaToIcebergJob.java` | ingest job (jar manifest main class): Kafka source → `IcebergSink` |
| `src/main/java/.../IcebergMaintenanceJob.java` | maintenance job (run via `-c`): standalone self-triggering `TableMaintenance` |
| `src/main/java/.../JsonToRowData.java` | JSON → Iceberg `RowData` mapper |
| `pom.xml` | deps + `maven-shade-plugin` (fat jar, `ServicesResourceTransformer`) |
| `docker-compose.yml` | MinIO, Postgres, Kafka, Flink JM/TM, `builder`, `submitter`, `datagen` |
| `scripts/datagen.sh` | kcat-based JSON event generator |

## Architecture invariants — do not break

* **Flink is pinned to the 2.0 line.** Iceberg ships a Flink connector only
  for Flink 2.0 (`iceberg-flink-runtime-2.0`). Do **not** bump `flink.version`
  past `2.0.x` unless a matching `iceberg-flink-runtime-<newer>` exists.
  Versions are cross-checked in `pom.xml` properties.
* **Two jobs, one jar.** Ingest (`KafkaToIcebergJob`, the jar's manifest main
  class) and maintenance (`IcebergMaintenanceJob`, submitted with `-c`) are
  deployed as separate Flink jobs. Shared schema/catalog/lock wiring lives in
  `IcebergCatalog`; keep it the single source of truth — don't duplicate
  catalog props or `SCHEMA` into the job classes. The split is *logical*: both
  run on the one TaskManager (no isolated memory budget — a second TM/cluster
  won't fit the 6 GB footprint). Maintenance is complete in-process: Iceberg
  1.10.2's Flink maintenance API includes `DeleteOrphanFiles`, so the job does
  compaction + expiration + orphan-file removal — don't reintroduce a claim
  that orphan removal needs another engine (it does not in this version).
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
* The Flink client (the `submitter` container) runs each job's `main()` up to
  `env.execute()`, so `IcebergCatalog.ensureTable()` runs there and needs
  network access to Postgres + MinIO. `depends_on` ordering in compose
  reflects this. `ensureTable()` is idempotent and swallows
  `AlreadyExistsException`, so the two jobs can be submitted in either order;
  the `submitter` submits ingest then maintenance.

## Gotchas

* Needs **≥ 6 GB memory available to Docker**. With less, the Flink
  TaskManager is OOM-killed (exit 137) → `NoResourceAvailableException`.
  Memory is tuned lean in `docker-compose.yml` (TM 1728m / JM 1000m / Kafka
  512m heap); keep it tuned if editing.
* Maintenance-startup JDBC cold-connect race (`JdbcLockFactory.open()` →
  `UncheckedSQLException`, thrown on the **TaskManager** operator). Three
  layers, keep all three — they do different jobs:
  (1) **`RetryingTriggerLockFactory`** wraps the JDBC lock and retries
  `open()` in-place on the TM (5 × 3 s) — this is what actually eliminates the
  restart; don't unwrap it.
  (2) `IcebergCatalog.awaitJdbc()` is only a client-side *fail-fast*
  pre-flight (clear error if Postgres is truly down); it cannot prevent the TM
  race — don't "upgrade" it expecting it to.
  (3) cluster `restart-strategy.type: fixed-delay` (3 / 5 s) is the bounded
  last resort.
  The `submitter` also submits maintenance only after ingest is RUNNING
  (deterministic order; not load-bearing for the race). Expected:
  `numRestarts=0`, empty exception history. A growing history / restart loop
  is the real signal — don't "fix" it by removing the JDBC lock.
* Re-running `submitter` submits another copy of *both* jobs; for a clean run
  do `docker compose down -v` first.
* All image tags **and** `pom.xml` deps/plugins are pinned to specific
  versions — keep them pinned (no `latest`/floating tags) when editing. MinIO
  archived its own Docker Hub repo in 2025: everything MinIO uses the
  maintained `alpine/minio` rebuild (needs `user: "0"` — it runs as uid 100 by
  default and can't write the root-owned named volume). `alpine/minio` has no
  `mc`; `minio-setup` creates the bucket via `mkdir /data/warehouse` (a
  top-level dir = a bucket in MinIO's single-drive backend) after minio
  formats the drive (`/data/.minio.sys` appears). Don't reintroduce `minio/mc`.

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
  var, which the compose `builder` forwards to `-Drevision`). The
  `flatten-maven-plugin` (`resolveCiFriendliesOnly`) resolves `${revision}` in
  the built POM and is required for ci-friendly versions on Maven 3 — don't
  remove it.
