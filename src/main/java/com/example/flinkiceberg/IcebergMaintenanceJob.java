package com.example.flinkiceberg;

import java.time.Duration;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.DeleteOrphanFiles;
import org.apache.iceberg.flink.maintenance.api.ExpireSnapshots;
import org.apache.iceberg.flink.maintenance.api.RewriteDataFiles;
import org.apache.iceberg.flink.maintenance.api.TableMaintenance;
import org.apache.iceberg.flink.maintenance.api.TriggerLockFactory;

/**
 * Maintenance job: a standalone, long-running Flink job whose entire graph is
 * Iceberg {@link TableMaintenance}. It has no Kafka source — it watches the
 * table's commit history and self-triggers the full set of maintenance tasks
 * on the configured cadence (commit count / age), so no external scheduler is
 * needed: data-file compaction ({@link RewriteDataFiles}), snapshot expiration
 * ({@link ExpireSnapshots}) and orphan-file removal
 * ({@link DeleteOrphanFiles}).
 *
 * <p>Deployed separately from {@link KafkaToIcebergJob} so maintenance and
 * ingest have independent lifecycles, failure domains and resource budgets.
 * Concurrent commits against the table are made safe by the Postgres-backed
 * {@link TriggerLockFactory} from {@link IcebergCatalog} — the same lock store
 * the ingest job's catalog uses, so the lock holds across both jobs.
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
        .add(
            // Least urgent and most expensive (it lists the whole table
            // prefix and HEADs candidates). Run on a long, infrequent interval
            // to MINIMISE — not eliminate — its inherent race with
            // ExpireSnapshots: orphan detection HEADs files
            // (BaseS3File.getObjectMetadata) that a concurrent ExpireSnapshots
            // run may delete underneath it, surfacing a transient S3 404
            // (NoSuchKey). That race is fundamental to running orphan removal
            // alongside another file-deleting task in the same job; it is
            // contained (the task's TaskResult is success=false, the job stays
            // RUNNING, it retries next interval) — see README "known
            // limitation". minAge stays well above the ingest commit cadence
            // so in-flight files are never mistaken for orphans;
            // usePrefixListing uses S3-style listing (correct for S3FileIO).
            DeleteOrphanFiles.builder()
                .scheduleOnInterval(Duration.ofMinutes(30))
                .minAge(Duration.ofMinutes(10))
                .usePrefixListing(true)
                .deleteBatchSize(100))
        .append();

    env.execute("iceberg-maintenance");
  }
}
