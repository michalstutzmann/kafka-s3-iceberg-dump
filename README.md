# Kafka S3 Iceberg Dump

A demo Apache Flink (Java) application that **continuously streams JSON events
from Kafka into an Apache Iceberg table on S3 (MinIO)**, with **Iceberg table
maintenance** (data-file compaction + snapshot expiration) running as a
**separate, self-triggering Flink job** built from the same jar, using the
[Flink table maintenance API](https://iceberg.apache.org/docs/nightly/flink-maintenance/).
Splitting ingest and maintenance into two jobs gives them independent
lifecycles, failure domains and tuning.

The whole thing — build, cluster, storage and a data generator — runs with a
single `docker compose up`.

## Stack (latest, mutually compatible — May 2026)

| Component | Version | Notes |
|---|---|---|
| Java | 21 | latest LTS supported by Flink 2.0 |
| Apache Flink | 2.0.2 | Iceberg ships a Flink connector for the 2.0 line |
| flink-connector-kafka | 4.0.1-2.0 | externalized Kafka connector for Flink 2.0 |
| Apache Iceberg | 1.10.2 | `iceberg-flink-runtime-2.0` + `iceberg-aws-bundle` |
| Apache Kafka | 4.0.0 | KRaft mode (no ZooKeeper) |
| Maven | 3.9.9 | runs in a container; no local JDK/Maven needed |
| MinIO / Postgres | alpine/minio RELEASE.2025-10-15 / pg 17.10 | S3 storage / Iceberg JDBC catalog + maintenance lock |

## Architecture

```
 datagen ──JSON──▶ Kafka topic "events"
                        │
                        ▼
        ┌─────────────────────────────────────┐
        │  Flink job 1: kafka-to-iceberg      │
        │  KafkaSource ─▶ JSON→RowData ─▶ IcebergSink ─┐
        └─────────────────────────────────────┘        │ commits
                                                        ▼
        ┌─────────────────────────────────────┐  Iceberg table db.events
        │  Flink job 2: iceberg-maintenance   │  warehouse on MinIO (S3)
        │  TableMaintenance (no Kafka source, ▲        │
        │  self-triggers on commit count/age):│        │ reads commits,
        │    • RewriteDataFiles (compaction)  └────────┘ rewrites/expires
        │    • ExpireSnapshots  (cleanup)              │
        └─────────────────────────────────────┘        │
                  catalog + maintenance lock: Postgres (Iceberg JDBC catalog)
```

Both jobs are built from one fat jar (`IcebergCatalog` holds the shared
schema/catalog/lock wiring) and run on the same Flink cluster. The
Postgres-backed `JdbcLockFactory` is honoured across both jobs, so compaction
and expiration never collide with ingest commits.

* **Catalog:** Iceberg **JDBC catalog** on Postgres. The same Postgres also
  backs the maintenance `JdbcLockFactory`, so compaction and expiration never
  run concurrently against the table.
* **Storage:** MinIO via Iceberg `S3FileIO` (path-style, `s3://warehouse`).
* **Table:** `db.events`, identity-partitioned by `event_type` so the stream
  produces many small files for `RewriteDataFiles` to compact.

## Prerequisites

* Docker / Docker Compose.
* **≥ 6 GB of memory available to Docker** (Docker Desktop → Settings →
  Resources). Flink TaskManager + JobManager + Kafka + MinIO + Postgres
  together need it; with less, the Flink TaskManager is OOM-killed (exit 137)
  and the job fails with `NoResourceAvailableException`. The stack is already
  tuned lean (TM 1728m, JM 1000m, Kafka 512m heap).

## Quick start

```bash
docker compose up --build
```

That will, in order:

1. `builder` — compile the shaded jar (`target/app.jar`) in a Maven container.
2. `minio` / `minio-setup` — start S3 and create the `warehouse` bucket.
3. `postgres` — Iceberg JDBC catalog + maintenance lock store.
4. `kafka` / `kafka-setup` — start the broker and create the `events` topic.
5. `flink-jobmanager` / `flink-taskmanager` — start the Flink 2.0 cluster.
6. `submitter` — submit both jobs (`kafka-to-iceberg`, then
   `iceberg-maintenance`) to the cluster.
7. `datagen` — stream synthetic JSON events into Kafka.

First run downloads images + Maven dependencies, so give it a few minutes.

## Verify it works

**Flink Web UI** — http://localhost:8081
Two running jobs: `kafka-to-iceberg` (KafkaSource → JSON→RowData →
IcebergSink) and `iceberg-maintenance` (the `RewriteDataFiles`,
`ExpireSnapshots`, lock/trigger operators — no Kafka source).

**MinIO console** — http://localhost:9001 (user `admin`, pass `password`)
Browse `warehouse/db/events/`:
* `data/event_type=*/…parquet` — file count **grows** as events arrive, then
  **drops** after a compaction run (RewriteDataFiles rewrites small files into
  ≤64 MB files every 3 commits).
* `metadata/` — new metadata/snapshot files per commit; old snapshots are
  pruned by ExpireSnapshots (keep last 3, max age 5 min).

**Maintenance logs**

```bash
docker compose logs -f flink-taskmanager | grep -Ei "rewrite|expire|maintenance"
```

**Catalog & lock in Postgres**

```bash
docker compose exec postgres \
  psql -U iceberg -d iceberg -c "\dt" -c "select * from iceberg_tables;"
```

**Peek at the raw Kafka stream (optional, from your laptop)**

```bash
docker compose exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic events --from-beginning --max-messages 5
```

## Tuning

Maintenance behaviour is set in
[`IcebergMaintenanceJob.java`](src/main/java/com/example/flinkiceberg/IcebergMaintenanceJob.java):

* `RewriteDataFiles.scheduleOnCommitCount(3)` / `targetFileSizeBytes(64MB)`
* `ExpireSnapshots.scheduleOnCommitCount(5)` / `maxSnapshotAge(5 min)` / `retainLast(3)`
* `TableMaintenance.rateLimit(1 min)` / `lockCheckDelay(10 s)`

Ingest behaviour (Kafka source, checkpoint interval) is in
[`KafkaToIcebergJob.java`](src/main/java/com/example/flinkiceberg/KafkaToIcebergJob.java).
Both jobs read all connection settings from environment variables
(`KAFKA_BOOTSTRAP`, `ICEBERG_JDBC_URI`, `S3_ENDPOINT`, …) with
container-friendly defaults — see
[`IcebergCatalog.fromEnv()`](src/main/java/com/example/flinkiceberg/IcebergCatalog.java).

## Build / run locally (without Docker)

```bash
./mvnw -DskipTests package          # produces target/app.jar (0.0.0-SNAPSHOT)

# ingest job (jar manifest main class):
flink run target/app.jar
# maintenance job (override the main class):
flink run -c com.example.flinkiceberg.IcebergMaintenanceJob target/app.jar
```

To stamp a real version, pass `-Drevision` from git-semver-release — see
[Versioning](#versioning).

Override the defaults via env vars, e.g. `KAFKA_BOOTSTRAP=localhost:29092`.

## Teardown

```bash
docker compose down -v   # also removes MinIO data, Postgres, and the built jar
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

For the Docker stack, pass it through the `APP_VERSION` env var (the `builder`
service forwards it to `-Drevision`):

```bash
APP_VERSION="$(git-semver-release version)" docker compose up --build
```

Plain `./mvnw package` / `docker compose up --build` (no version supplied) just
falls back to `0.0.0-SNAPSHOT` — fine for the demo, since the fat jar is always
emitted as `target/app.jar` regardless of version.

## Notes / troubleshooting

* **All images are pinned** to specific versions for reproducibility (see
  `docker-compose.yml`). MinIO archived its own Docker Hub repo in 2025, so
  everything MinIO-related uses the actively-maintained `alpine/minio`
  community rebuild (security-patched Oct-2025 release; runs as root via
  `user: "0"` so it can write the named volume). No `mc` / `minio/mc` image is
  used at all — the `minio-setup` job creates the bucket with a plain `mkdir`
  on the data volume (in MinIO's single-drive backend a top-level directory
  *is* a bucket), reusing `alpine/minio`'s own shell.
* The Flink **client** (the `submitter` container) executes each job's
  `main()` up to `env.execute()`, so `IcebergCatalog.ensureTable()` runs there
  and needs network access to Postgres and MinIO — `depends_on` handles
  ordering. `ensureTable()` is idempotent and race-tolerant, so the ingest and
  maintenance jobs can be submitted in either order.
* Both jobs run on the **same single TaskManager**, so this is a *logical*
  split (independent job lifecycles + failure domains, independently tunable
  cadence) — it does **not** give maintenance an isolated memory budget. True
  resource isolation would need a second TaskManager/cluster, which the lean
  6 GB demo footprint does not have room for. Note also that Flink's
  maintenance API does not do orphan-file removal — a known gap for a
  Flink-only stack.
* Iceberg's `CatalogLoader` requires `org.apache.hadoop.conf.Configuration` on
  the classpath even with `S3FileIO`; the shaded `hadoop-client-api/runtime`
  uber jars are bundled to satisfy that without dragging in a full Hadoop tree.
* The `iceberg-maintenance` job's `JdbcLockFactory.open()` (run on the
  *TaskManager* operator) could lose a cold-connect race to Postgres on first
  deploy (`UncheckedSQLException: Failed to connect`), costing one startup
  restart. Defence in depth, in order of who actually fixes it:
  1. **`RetryingTriggerLockFactory`** (the real fix) wraps the lock factory and
     retries `open()` *in-place on the TaskManager* (5 × 3 s), absorbing the
     transient with no job restart. A client-side pre-flight can't do this —
     the failing connection is made on the TM, not the submitter.
  2. **`IcebergCatalog.awaitJdbc()`** is a *fail-fast* pre-flight in the
     submitter: if Postgres is genuinely down it errors clearly before any job
     is created (it does not prevent the TM race).
  3. **`restart-strategy.type: fixed-delay`** (3 attempts, 5 s) is the bounded
     last resort, so anything still unhandled recovers fast / fails fast
     instead of looping forever.

  Expected steady state: **`numRestarts=0`** for both jobs and an empty
  exception history. A *growing* exception history or a restart loop is the
  real signal something is wrong.
* Re-running `submitter` submits another copy of *both* jobs; for a clean
  restart use `docker compose down -v` then `docker compose up --build`.
