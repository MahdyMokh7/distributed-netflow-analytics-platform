package com.nimbo.analyticsquery.routing;

import com.nimbo.analyticsquery.model.AnalysisDataSource;
import com.nimbo.analyticsquery.schema.SemanticTable;
import java.util.Objects;

public record DataSourceRoute(AnalysisDataSource dataSource, SemanticTable table) {

  public DataSourceRoute {
    Objects.requireNonNull(dataSource, "dataSource must not be null");
    Objects.requireNonNull(table, "table must not be null");

    if (table.dataSource() != dataSource) {
      throw new IllegalArgumentException("table must belong to the selected datasource");
    }
  }
}
