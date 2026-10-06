package com.nimbo.analyticsquery.sql;

import com.nimbo.analyticsquery.model.AnalysisDataSource;
import java.util.Objects;

public record GeneratedSqlCandidate(AnalysisDataSource dataSource, String sql) {

  public GeneratedSqlCandidate {
    Objects.requireNonNull(dataSource, "dataSource must not be null");

    if (sql == null || sql.isBlank()) {
      throw new IllegalArgumentException("sql must not be blank");
    }
  }
}
