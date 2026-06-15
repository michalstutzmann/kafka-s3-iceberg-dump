package com.example.flinkiceberg;

import java.time.Duration;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.DeleteOrphanFiles;
import org.apache.iceberg.flink.maintenance.api.TableMaintenance;
import org.apache.iceberg.flink.maintenance.api.TriggerLockFactory;

/**
 * Orphan-file GC job: a {@link TableMaintenance} graph with a single
 * {@link DeleteOrphanFiles} task.
 *
 * <p>The cluster is suspended by default and woken by the
 * {@code orphan-gc-runner} CronJob (see {@code k8s/maintenance-cron.yaml}).
 * The in-job interval and rateLimit are tuned for that wake model rather
 * than for a long-running watch: empirically, Iceberg's TableMaintenance
 * gates the first trigger fire after a stateless cold start by
 * {@code max(scheduleInterval, rateLimit)} for interval-based triggers,
 * because both the trigger's {@code lastTriggerTime} and the rate limiter's
 * {@code lastFireTime} are initialised to job-start. So both values MUST
 * stay shorter than the cron wake window or the pass never fires. With
 * {@code scheduleOnInterval(20s)} + {@code rateLimit(1min)} and a 240-second
 * wake, the first orphan pass fires ~60 s after TaskManager start
 * (well inside the wake) and may fire once more before the cron suspends
 * the cluster — both passes are idempotent so the extra fire is harmless.
 * Cron is the real schedule; these values just guarantee one in-wake pass.
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
        // BOTH rateLimit AND scheduleOnInterval must be shorter than the
        // cron wake window — Iceberg gates the first fire on a stateless
        // cold start by max(interval, rateLimit). Match the maintenance
        // job's 1-minute rateLimit so first fire happens ~60 s after TM
        // start. A second fire may slip in before the cron's suspend; the
        // orphan pass is idempotent so that's harmless.
        .rateLimit(Duration.ofMinutes(1))
        .lockCheckDelay(Duration.ofSeconds(10))
        .add(
            // Lock-serialised against rewrite/expire, so it no longer races
            // ExpireSnapshots' deletes. minAge stays well above the ingest
            // commit cadence so in-flight files are never mistaken for
            // orphans; usePrefixListing uses S3-style listing (correct for
            // the S3FileIO/MinIO store).
            DeleteOrphanFiles.builder()
                .scheduleOnInterval(Duration.ofSeconds(20))
                .minAge(Duration.ofMinutes(10))
                .usePrefixListing(true)
                .deleteBatchSize(100))
        .append();

    env.execute("iceberg-orphan-gc");
  }
}
