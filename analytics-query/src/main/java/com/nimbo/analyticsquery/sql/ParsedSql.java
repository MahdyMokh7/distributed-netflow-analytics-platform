package com.nimbo.analyticsquery.sql;

import com.nimbo.analyticsquery.model.AnalysisDataSource;
import java.util.List;
import java.util.Objects;
import net.sf.jsqlparser.statement.Statement;

public record ParsedSql(AnalysisDataSource dataSource, String sql, List<Statement> statements) {

  public ParsedSql {
    Objects.requireNonNull(dataSource, "dataSource must not be null");

    if (sql == null || sql.isBlank()) {
      throw new IllegalArgumentException("sql must not be blank");
    }

    statements = List.copyOf(Objects.requireNonNull(statements, "statements must not be null"));
  }
}
