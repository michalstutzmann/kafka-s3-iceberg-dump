# Kafka S3 Iceberg Dump

A demo Apache Flink (Java) application that **continuously streams JSON events
from Kafka into an Apache Iceberg table on S3 (MinIO)**, while running
**Iceberg table maintenance** (data-file compaction + snapshot expiration)
*inside the same Flink job* using the
[Flink table maintenance API](https://iceberg.apache.org/docs/nightly/flink-maintenance/).

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
        ┌──────────────────────────────────────┐
        │            Flink job                  │
        │  KafkaSource ─▶ JSON→RowData ─▶ IcebergSink ─┐
        │                                              │ commits
        │  TableMaintenance (same StreamEnv):          ▼
        │    • RewriteDataFiles  (compaction)   Iceberg table db.events
        │    • ExpireSnapshots   (cleanup)      warehouse on MinIO (S3)
        └──────────────────────────────────────┘
                  catalog + lock: Postgres (Iceberg JDBC catalog)
```

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
6. `submitter` — submit the job to the cluster.
7. `datagen` — stream synthetic JSON events into Kafka.

First run downloads images + Maven dependencies, so give it a few minutes.

## Verify it works

**Flink Web UI** — http://localhost:8081
The running `kafka-to-iceberg` job shows two pipelines: the Kafka→Iceberg
ingest, and the Iceberg maintenance operators (`RewriteDataFiles`,
`ExpireSnapshots`, the lock/trigger operators).

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

Maintenance/ingest behaviour is set in
[`KafkaToIcebergJob.java`](src/main/java/com/example/flinkiceberg/KafkaToIcebergJob.java):

* `RewriteDataFiles.scheduleOnCommitCount(3)` / `targetFileSizeBytes(64MB)`
* `ExpireSnapshots.scheduleOnCommitCount(5)` / `maxSnapshotAge(5 min)` / `retainLast(3)`
* `TableMaintenance.rateLimit(1 min)` / `lockCheckDelay(10 s)`

The job reads all connection settings from environment variables
(`KAFKA_BOOTSTRAP`, `ICEBERG_JDBC_URI`, `S3_ENDPOINT`, …) with
container-friendly defaults — see the top of `main()`.

## Build / run locally (without Docker)

```bash
./mvnw -DskipTests package          # produces target/app.jar (0.0.0-SNAPSHOT)
flink run target/app.jar            # against any Flink 2.0 cluster
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
* The Flink **client** (the `submitter` container) executes `main()` up to
  `env.execute()`, so it creates the namespace/table and therefore needs
  network access to Postgres and MinIO — `depends_on` handles ordering.
* Iceberg's `CatalogLoader` requires `org.apache.hadoop.conf.Configuration` on
  the classpath even with `S3FileIO`; the shaded `hadoop-client-api/runtime`
  uber jars are bundled to satisfy that without dragging in a full Hadoop tree.
* Re-running `submitter` submits another job copy; for a clean restart use
  `docker compose down -v` then `docker compose up --build`.
