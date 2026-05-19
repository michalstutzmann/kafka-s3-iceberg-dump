package com.example.flinkiceberg;

import java.time.Duration;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.DeleteOrphanFiles;
import org.apache.iceberg.flink.maintenance.api.TableMaintenance;
import org.apache.iceberg.flink.maintenance.api.TriggerLockFactory;

/**
 * Orphan-file GC job: a standalone, long-running Flink job whose entire
 * {@link TableMaintenance} graph is a single {@link DeleteOrphanFiles} task,
 * self-triggered on a long interval.
 *
 * <p>Why its own job/cluster: orphan detection HEADs files
 * ({@code BaseS3File.getObjectMetadata}); when {@code ExpireSnapshots} (in
 * {@link IcebergMaintenanceJob}) deletes snapshot/manifest files in the same
 * window, the HEAD hits a transient S3 404 and the orphan pass fails. Running
 * it here, in its own graph, lets the <em>shared</em> Postgres maintenance
 * lock do its job: this job's {@link TableMaintenance} acquires the same lock
 * id ({@code db.events}) from {@link IcebergCatalog#lockFactory()} that the
 * rewrite/expire job uses, so the lock factory serialises orphan GC against
 * rewrite/expire — they can no longer delete files underneath each other.
 * It also gets an isolated cluster (own JM+TM), consistent with the rest of
 * the design.
 *
 * <p>{@code lockCheckDelay} governs how often this job re-checks the shared
 * lock when the maintenance job is mid-cycle and holding it.
 */
public final class IcebergOrphanGcJob {

  private IcebergOrphanGcJob() {}

  public static void main(String[] args) throws Exception {
    IcebergCatalog catalog = IcebergCatalog.fromEnv();
    // Fail-fast pre-flight; the TaskManager cold-connect race is absorbed
    // in-place by RetryingTriggerLockFactory (see IcebergCatalog.lockFactory()).
    catalog.awaitJdbc(30, Duration.ofSeconds(2));
    // Idempotent and race-tolerant: any of the jobs may be deployed first.
    catalog.ensureTable();
    TableLoader tableLoader = catalog.tableLoader();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // The TableMaintenance source keeps state about which snapshots it has
    // already evaluated; checkpointing lets that survive job restarts.
    env.enableCheckpointing(Duration.ofSeconds(30).toMillis());

    // Same lock id (db.events) as IcebergMaintenanceJob -> the Postgres lock
    // serialises this against that job's rewrite/expire cycles.
    TriggerLockFactory lockFactory = catalog.lockFactory();

    TableMaintenance.forTable(env, tableLoader, lockFactory)
        .uidSuffix("iceberg-orphan-gc")
        .rateLimit(Duration.ofMinutes(1))
        .lockCheckDelay(Duration.ofSeconds(10))
        .add(
            // Lock-serialised against rewrite/expire, so it no longer races
            // ExpireSnapshots' deletes. minAge stays well above the ingest
            // commit cadence so in-flight files are never mistaken for
            // orphans; usePrefixListing uses S3-style listing (correct for
            // the S3FileIO/MinIO store).
            DeleteOrphanFiles.builder()
                .scheduleOnInterval(Duration.ofMinutes(15))
                .minAge(Duration.ofMinutes(10))
                .usePrefixListing(true)
                .deleteBatchSize(100))
        .append();

    env.execute("iceberg-orphan-gc");
  }
}
