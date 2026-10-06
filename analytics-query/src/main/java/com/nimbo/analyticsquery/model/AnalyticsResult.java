package com.nimbo.analyticsquery.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public record AnalyticsResult(
    AnalysisDataSource dataSource,
    ExecutionMode executionMode,
    List<String> columns,
    List<Map<String, Object>> rows,
    long rowCount,
    Map<String, Object> metadata) {

  public AnalyticsResult {
    Objects.requireNonNull(dataSource, "dataSource must not be null");
    Objects.requireNonNull(executionMode, "executionMode must not be null");
    Objects.requireNonNull(columns, "columns must not be null");
    Objects.requireNonNull(rows, "rows must not be null");
    Objects.requireNonNull(metadata, "metadata must not be null");

    columns = List.copyOf(columns);

    rows = rows.stream().map(row -> Collections.unmodifiableMap(new LinkedHashMap<>(row))).toList();

    metadata = Collections.unmodifiableMap(new LinkedHashMap<>(metadata));

    if (rowCount != rows.size()) {
      throw new IllegalArgumentException("rowCount must match rows size");
    }
  }

  public static AnalyticsResult postgresTool(
      List<String> columns, List<Map<String, Object>> rows, Map<String, Object> metadata) {

    return new AnalyticsResult(
        AnalysisDataSource.POSTGRESQL, ExecutionMode.TOOL, columns, rows, rows.size(), metadata);
  }

  public static AnalyticsResult generatedSql(
      AnalysisDataSource dataSource, List<String> columns, List<Map<String, Object>> rows) {

    return new AnalyticsResult(
        dataSource, ExecutionMode.GENERATED_SQL, columns, rows, rows.size(), Map.of());
  }

  public static AnalyticsResult emptyPostgresTool(List<String> columns) {

    return postgresTool(columns, List.of(), Map.of());
  }
}
