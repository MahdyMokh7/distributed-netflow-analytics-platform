package com.nimbo.cubemaker.spark;

import java.util.UUID;
import java.util.function.Supplier;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

/** Adds batch-aware metadata to Spark actions so they are identifiable in the Spark UI. */
public final class SparkJobContext {

  private static final String GROUP_PREFIX = "cube-maker:";

  private SparkJobContext() {}

  public static <T> T execute(
      SparkSession spark,
      UUID batchId,
      String action,
      String description,
      Supplier<T> operation) {
    SparkContext sparkContext = spark.sparkContext();
    sparkContext.setJobGroup(
        GROUP_PREFIX + batchId + ":" + action,
        description + " [batch=" + batchId + "]",
        false);
    try {
      return operation.get();
    } finally {
      sparkContext.clearJobGroup();
    }
  }

  public static void execute(
      SparkSession spark, UUID batchId, String action, String description, Runnable operation) {
    execute(
        spark,
        batchId,
        action,
        description,
        () -> {
          operation.run();
          return null;
        });
  }
}
