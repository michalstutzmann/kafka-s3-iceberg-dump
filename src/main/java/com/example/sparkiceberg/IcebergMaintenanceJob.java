package com.example.sparkiceberg;

import java.time.Duration;

import org.apache.iceberg.Table;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.SparkSession;

/** Runs Spark Iceberg maintenance actions: data-file rewrite and snapshot expiration. */
public final class IcebergMaintenanceJob {

  private IcebergMaintenanceJob() {}

  public static void main(String[] args) throws Exception {
    IcebergCatalog catalog = IcebergCatalog.fromEnv();
    catalog.awaitJdbc(30, Duration.ofSeconds(2));

    SparkSession spark = catalog.sparkSession("iceberg-maintenance");
    try {
      catalog.withMaintenanceLock(
          () -> {
            Table table = catalog.loadTable();

            System.out.println("Running RewriteDataFiles for " + catalog.tableName());
            Object rewriteResult =
                SparkActions.get(spark)
                    .rewriteDataFiles(table)
                    .option("target-file-size-bytes", Long.toString(64L * 1024 * 1024))
                    .option("partial-progress.enabled", "true")
                    .option("partial-progress.max-commits", "2")
                    .execute();
            System.out.println("RewriteDataFiles result: " + rewriteResult);

            table.refresh();
            System.out.println("Running ExpireSnapshots for " + catalog.tableName());
            Object expireResult =
                SparkActions.get(spark)
                    .expireSnapshots(table)
                    .expireOlderThan(System.currentTimeMillis() - Duration.ofMinutes(5).toMillis())
                    .retainLast(3)
                    .execute();
            System.out.println("ExpireSnapshots result: " + expireResult);
          });
    } finally {
      spark.stop();
    }
  }
}
