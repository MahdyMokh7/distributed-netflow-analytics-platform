package com.nimbo.analyticsquery.tool;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

final class ToolResultRows {

  private ToolResultRows() {}

  static Map<String, Object> fromColumns(List<String> columns, Object... values) {
    Objects.requireNonNull(columns, "columns must not be null");
    Objects.requireNonNull(values, "values must not be null");

    if (columns.size() != values.length) {
      throw new IllegalArgumentException("column and value counts must match");
    }

    Map<String, Object> row = new LinkedHashMap<>();
    for (int index = 0; index < columns.size(); index++) {
      row.put(columns.get(index), values[index]);
    }
    return row;
  }
}
