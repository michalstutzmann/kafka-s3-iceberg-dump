package com.example.flinkiceberg;

import java.time.Duration;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.ExpireSnapshots;
import org.apache.iceberg.flink.maintenance.api.RewriteDataFiles;
import org.apache.iceberg.flink.maintenance.api.TableMaintenance;
import org.apache.iceberg.flink.maintenance.api.TriggerLockFactory;

/**
 * Maintenance job: a standalone, long-running Flink job whose entire graph is
 * Iceberg {@link TableMaintenance}. It has no Kafka source — it watches the
 * table's commit history and self-triggers data-file compaction
 * ({@link RewriteDataFiles}) and snapshot expiration
 * ({@link ExpireSnapshots}) on the configured cadence, so no external
 * scheduler is needed.
 *
 * <p>Orphan-file removal is intentionally <em>not</em> here: it ran in this
 * same graph and its file HEADs raced {@link ExpireSnapshots}'s concurrent
 * deletes (transient S3 404s). It now lives in {@link IcebergOrphanGcJob} —
 * a separate job/cluster that shares the <em>same</em> Postgres maintenance
 * lock ({@link IcebergCatalog#lockFactory()}, lock id {@code db.events}), so
 * the lock serialises orphan GC against this job's rewrite/expire cycles.
 *
 * <p>Deployed separately from {@link KafkaToIcebergJob} so maintenance and
 * ingest have independent lifecycles, failure domains and resource budgets.
 * Concurrent commits against the table are made safe by the Postgres-backed
 * {@link TriggerLockFactory} from {@link IcebergCatalog} — the same lock store
 * the ingest job's catalog uses, so the lock holds across all jobs.
 */
public final class IcebergMaintenanceJob {

  private IcebergMaintenanceJob() {}

  public static void main(String[] args) throws Exception {
    IcebergCatalog catalog = IcebergCatalog.fromEnv();
    // Fail-fast pre-flight: clear error from the submitter if Postgres is
    // genuinely down. The TaskManager cold-connect race is absorbed in-place
    // by RetryingTriggerLockFactory (see IcebergCatalog.lockFactory()).
    catalog.awaitJdbc(30, Duration.ofSeconds(2));
    // Idempotent and race-tolerant: whichever of the ingest/maintenance jobs
    // is deployed first creates the table; the other just loads it.
    catalog.ensureTable();
    TableLoader tableLoader = catalog.tableLoader();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // The TableMaintenance source keeps state about which snapshots it has
    // already evaluated; checkpointing lets that survive job restarts.
    env.enableCheckpointing(Duration.ofSeconds(30).toMillis());

    TriggerLockFactory lockFactory = catalog.lockFactory();

    TableMaintenance.forTable(env, tableLoader, lockFactory)
        .uidSuffix("iceberg-maintenance")
        .rateLimit(Duration.ofMinutes(1))
        .lockCheckDelay(Duration.ofSeconds(10))
        .add(
            RewriteDataFiles.builder()
                .scheduleOnCommitCount(3)
                .targetFileSizeBytes(64L * 1024 * 1024)
                .partialProgressEnabled(true)
                .partialProgressMaxCommits(2))
        .add(
            ExpireSnapshots.builder()
                .scheduleOnCommitCount(5)
                .maxSnapshotAge(Duration.ofMinutes(5))
                .retainLast(3)
                .deleteBatchSize(100))
        .append();

    env.execute("iceberg-maintenance");
  }
}
