package com.nimbo.backfill.sql;

import com.nimbo.backfill.config.BackfillConfig;
import com.nimbo.backfill.model.BackfillProjection;
import com.nimbo.backfill.model.DailyBackfillPlan;
import com.nimbo.backfill.model.HdfsPathColumnMapping;
import java.util.List;
import java.util.stream.Collectors;

public final class NetflowBackfillSql {

  private NetflowBackfillSql() {}

  public static String describeTable(BackfillConfig config) {
    return "DESC " + starRocksTable(config);
  }

  public static String describeHdfs(BackfillConfig config, String path) {
    return "DESC " + files(config, path);
  }

  public static String stagingPartitionExists(BackfillConfig config, DailyBackfillPlan plan) {
    return "SHOW TEMPORARY PARTITIONS FROM "
        + starRocksTable(config)
        + " WHERE PartitionName = "
        + literal(plan.stagingPartitionName());
  }

  public static String sourceIntervalMetrics(
      BackfillConfig config, DailyBackfillPlan plan, String path) {
    return """
           SELECT COUNT(*) AS row_count,
                  COALESCE(SUM(IN_BYTES), 0) AS in_bytes,
                  COALESCE(SUM(OUT_BYTES), 0) AS out_bytes,
                  COALESCE(SUM(IN_PKTS), 0) AS in_packets,
                  COALESCE(SUM(OUT_PKTS), 0) AS out_packets,
                  MIN(CAST(PROCESS_TIME AS BIGINT)) AS minimum_timestamp,
                  MAX(CAST(PROCESS_TIME AS BIGINT)) AS maximum_timestamp
           FROM %s
           WHERE PROCESS_TIME >= %d
             AND PROCESS_TIME < %d
           """
        .formatted(files(config, path), plan.fromMillis(), plan.toMillis());
  }

  public static String targetIntervalMetrics(BackfillConfig config, DailyBackfillPlan plan) {
    return destinationMetrics(config, "", plan);
  }

  public static String stagingIntervalMetrics(BackfillConfig config, DailyBackfillPlan plan) {
    return destinationMetrics(
        config, "TEMPORARY PARTITION (" + identifier(plan.stagingPartitionName()) + ")", plan);
  }

  public static String dropStagingPartition(BackfillConfig config, DailyBackfillPlan plan) {
    return "ALTER TABLE "
        + starRocksTable(config)
        + " DROP TEMPORARY PARTITION "
        + identifier(plan.stagingPartitionName());
  }

  public static String createStagingPartition(BackfillConfig config, DailyBackfillPlan plan) {
    return "ALTER TABLE "
        + starRocksTable(config)
        + " ADD TEMPORARY PARTITION "
        + identifier(plan.stagingPartitionName())
        + " VALUES IN (("
        + literal(plan.day() + " 00:00:00")
        + "))";
  }

  public static String publishStagedInterval(
      BackfillConfig config, DailyBackfillPlan plan, BackfillProjection projection) {
    String columns = columns(projection.starRocksColumnNames());
    return """
           INSERT INTO %s (
           %s
           )
           SELECT
           %s
           FROM %s TEMPORARY PARTITION (%s)
           WHERE PROCESS_TIME >= %d
             AND PROCESS_TIME < %d
           """
        .formatted(
            starRocksTable(config),
            columns,
            columns,
            starRocksTable(config),
            identifier(plan.stagingPartitionName()),
            plan.fromMillis(),
            plan.toMillis());
  }

  public static String deleteTargetInterval(BackfillConfig config, DailyBackfillPlan plan) {
    return """
           DELETE FROM %s
           WHERE PROCESS_TIME >= %d
             AND PROCESS_TIME < %d
           """
        .formatted(starRocksTable(config), plan.fromMillis(), plan.toMillis());
  }

  public static String loadSourceIntoStaging(
      BackfillConfig config, DailyBackfillPlan plan, BackfillProjection projection) {
    return insertFromHdfs(
        config,
        " TEMPORARY PARTITION (" + identifier(plan.stagingPartitionName()) + ")",
        plan,
        projection);
  }

  public static String validateSourceProjection(
      BackfillConfig config, DailyBackfillPlan plan, HdfsPathColumnMapping hdfsColumnMapping) {
    return hdfsSelect(config, plan, hdfsColumnMapping) + "LIMIT 1";
  }

  public static String starRocksTable(BackfillConfig config) {
    return identifier(config.database()) + "." + identifier(config.table());
  }

  private static String insertFromHdfs(
      BackfillConfig config,
      String partitionClause,
      DailyBackfillPlan plan,
      BackfillProjection projection) {
    String selects =
        projection.hdfsColumnMappings().stream()
            .map(hdfsColumnMapping -> hdfsSelect(config, plan, hdfsColumnMapping))
            .collect(Collectors.joining("UNION ALL\n"));
    return """
           INSERT INTO %s%s (
           %s
           )
           %s
           """
        .formatted(
            starRocksTable(config),
            partitionClause,
            columns(projection.starRocksColumnNames()),
            selects);
  }

  private static String hdfsSelect(
      BackfillConfig config, DailyBackfillPlan plan, HdfsPathColumnMapping hdfsColumnMapping) {
    return """
           SELECT
           %s
           FROM %s
           WHERE PROCESS_TIME >= %d
             AND PROCESS_TIME < %d
           """
        .formatted(
            String.join(",\n", hdfsColumnMapping.expressions()),
            files(config, hdfsColumnMapping.path()),
            plan.fromMillis(),
            plan.toMillis());
  }

  private static String destinationMetrics(
      BackfillConfig config, String partitionClause, DailyBackfillPlan plan) {
    return """
           SELECT COUNT(*) AS row_count,
                  COALESCE(SUM(in_bytes), 0) AS in_bytes,
                  COALESCE(SUM(out_bytes), 0) AS out_bytes,
                  COALESCE(SUM(in_pkts), 0) AS in_packets,
                  COALESCE(SUM(out_pkts), 0) AS out_packets,
                  MIN(PROCESS_TIME) AS minimum_timestamp,
                  MAX(PROCESS_TIME) AS maximum_timestamp
           FROM %s %s
           WHERE PROCESS_TIME >= %d
             AND PROCESS_TIME < %d
           """
        .formatted(starRocksTable(config), partitionClause, plan.fromMillis(), plan.toMillis());
  }

  private static String files(BackfillConfig config, String path) {
    return """
           FILES(
               "path" = %s,
               "format" = "parquet",
               "hadoop.security.authentication" = "simple",
               "username" = %s,
               "fill_mismatch_column_with" = "null",
               "auto_detect_sample_files" = "1000"
           )
           """
        .formatted(literal(path), literal(config.hdfsUsername()));
  }

  private static String columns(List<String> names) {
    return names.stream().map(NetflowBackfillSql::identifier).collect(Collectors.joining(",\n"));
  }

  private static String identifier(String value) {
    return "`" + value.replace("`", "``") + "`";
  }

  private static String literal(String value) {
    return "'" + value.replace("'", "''") + "'";
  }
}
