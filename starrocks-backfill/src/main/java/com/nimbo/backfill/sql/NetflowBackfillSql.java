package com.nimbo.backfill.sql;

import java.util.List;
import java.util.stream.Collectors;

import com.nimbo.backfill.config.BackfillConfig;
import com.nimbo.backfill.model.BackfillProjection;
import com.nimbo.backfill.model.BackfillProjection.HdfsPathProjection;
import com.nimbo.backfill.model.DailyBackfillPlan;

public final class NetflowBackfillSql {

    private NetflowBackfillSql() {
    }

    public static String describeTable(BackfillConfig config) {
        return "DESC " + qualifiedTable(config);
    }

    public static String describeHdfs(BackfillConfig config, String path) {
        return "DESC " + files(config, path);
    }

    public static String temporaryPartitionExists(BackfillConfig config, DailyBackfillPlan plan) {
        return "SHOW TEMPORARY PARTITIONS FROM " + qualifiedTable(config) + " WHERE PartitionName = "
                + literal(plan.temporaryPartition());
    }

    public static String sourceMetrics(BackfillConfig config, DailyBackfillPlan plan, String path) {
        return """
                SELECT COUNT(*) AS row_count,
                       COALESCE(SUM(IN_BYTES), 0) AS in_bytes,
                       COALESCE(SUM(OUT_BYTES), 0) AS out_bytes,
                       COALESCE(SUM(IN_PKTS), 0) AS in_packets,
                       COALESCE(SUM(OUT_PKTS), 0) AS out_packets,
                       MIN(CAST(PROCESS_TIME / 1000 AS BIGINT)) AS minimum_timestamp,
                       MAX(CAST(PROCESS_TIME / 1000 AS BIGINT)) AS maximum_timestamp
                FROM %s
                WHERE PROCESS_TIME >= %d
                  AND PROCESS_TIME < %d
                """.formatted(files(config, path), plan.fromMicros(), plan.toMicros());
    }

    public static String formalIntervalMetrics(BackfillConfig config, DailyBackfillPlan plan) {
        return destinationMetrics(config, "", plan);
    }

    public static String temporaryIntervalMetrics(BackfillConfig config, DailyBackfillPlan plan) {
        return destinationMetrics(config, "TEMPORARY PARTITION (" + identifier(plan.temporaryPartition()) + ")", plan);
    }

    public static String dropTemporaryPartition(BackfillConfig config, DailyBackfillPlan plan) {
        return "ALTER TABLE " + qualifiedTable(config) + " DROP TEMPORARY PARTITION "
                + identifier(plan.temporaryPartition());
    }

    public static String createTemporaryPartition(BackfillConfig config, DailyBackfillPlan plan) {
        return "ALTER TABLE " + qualifiedTable(config) + " ADD TEMPORARY PARTITION "
                + identifier(plan.temporaryPartition()) + " VALUES IN ((" + literal(plan.day() + " 00:00:00") + "))";
    }

    public static String copyTemporaryIntervalIntoFormal(BackfillConfig config, DailyBackfillPlan plan,
            BackfillProjection projection) {
        String columns = columns(projection.tableColumns());
        return """
                INSERT INTO %s (
                %s
                )
                SELECT
                %s
                FROM %s TEMPORARY PARTITION (%s)
                WHERE kafka_timestamp >= %d
                  AND kafka_timestamp < %d
                """.formatted(qualifiedTable(config), columns, columns, qualifiedTable(config),
                identifier(plan.temporaryPartition()), plan.fromMillis(), plan.toMillis());
    }

    public static String deleteFormalInterval(BackfillConfig config, DailyBackfillPlan plan) {
        return """
                DELETE FROM %s
                WHERE kafka_timestamp >= %d
                  AND kafka_timestamp < %d
                """.formatted(qualifiedTable(config), plan.fromMillis(), plan.toMillis());
    }

    public static String loadHdfs(BackfillConfig config, DailyBackfillPlan plan, BackfillProjection projection) {
        return insertFromHdfs(config, " TEMPORARY PARTITION (" + identifier(plan.temporaryPartition()) + ")", plan,
                projection);
    }

    public static String validateHdfsProjection(BackfillConfig config, DailyBackfillPlan plan,
            HdfsPathProjection pathProjection) {
        return hdfsSelect(config, plan, pathProjection) + "LIMIT 1";
    }

    public static String qualifiedTable(BackfillConfig config) {
        return identifier(config.database()) + "." + identifier(config.table());
    }

    private static String insertFromHdfs(BackfillConfig config, String partitionClause, DailyBackfillPlan plan,
            BackfillProjection projection) {
        String selects = projection.pathProjections().stream()
                .map(pathProjection -> hdfsSelect(config, plan, pathProjection))
                .collect(Collectors.joining("UNION ALL\n"));
        return """
                INSERT INTO %s%s (
                %s
                )
                %s
                """.formatted(qualifiedTable(config), partitionClause, columns(projection.tableColumns()), selects);
    }

    private static String hdfsSelect(BackfillConfig config, DailyBackfillPlan plan,
            HdfsPathProjection pathProjection) {
        return """
                SELECT
                %s
                FROM %s
                WHERE PROCESS_TIME >= %d
                  AND PROCESS_TIME < %d
                """.formatted(String.join(",\n", pathProjection.expressions()),
                files(config, pathProjection.path()), plan.fromMicros(), plan.toMicros());
    }

    private static String destinationMetrics(BackfillConfig config, String partitionClause, DailyBackfillPlan plan) {
        return """
                SELECT COUNT(*) AS row_count,
                       COALESCE(SUM(in_bytes), 0) AS in_bytes,
                       COALESCE(SUM(out_bytes), 0) AS out_bytes,
                       COALESCE(SUM(in_pkts), 0) AS in_packets,
                       COALESCE(SUM(out_pkts), 0) AS out_packets,
                       MIN(kafka_timestamp) AS minimum_timestamp,
                       MAX(kafka_timestamp) AS maximum_timestamp
                FROM %s %s
                WHERE kafka_timestamp >= %d
                  AND kafka_timestamp < %d
                """.formatted(qualifiedTable(config), partitionClause, plan.fromMillis(), plan.toMillis());
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
                """.formatted(literal(path), literal(config.hdfsUsername()));
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
