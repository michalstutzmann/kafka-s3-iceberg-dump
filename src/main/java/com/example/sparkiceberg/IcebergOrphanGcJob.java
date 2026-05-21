package com.example.sparkiceberg;

import java.time.Duration;

import org.apache.iceberg.Table;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.SparkSession;

/** Runs Spark Iceberg orphan-file removal as a separate batch job. */
public final class IcebergOrphanGcJob {

  private IcebergOrphanGcJob() {}

  public static void main(String[] args) throws Exception {
    IcebergCatalog catalog = IcebergCatalog.fromEnv();
    catalog.awaitJdbc(30, Duration.ofSeconds(2));

    SparkSession spark = catalog.sparkSession("iceberg-orphan-gc");
    try {
      catalog.withMaintenanceLock(
          () -> {
            Table table = catalog.loadTable();
            System.out.println("Running DeleteOrphanFiles for " + catalog.tableName());
            Object result =
                SparkActions.get(spark)
                    .deleteOrphanFiles(table)
                    .olderThan(System.currentTimeMillis() - Duration.ofMinutes(10).toMillis())
                    .usePrefixListing(true)
                    .execute();
            System.out.println("DeleteOrphanFiles result: " + result);
          });
    } finally {
      spark.stop();
    }
  }
}
