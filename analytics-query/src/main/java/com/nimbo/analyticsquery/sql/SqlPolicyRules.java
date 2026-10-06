package com.nimbo.analyticsquery.sql;

import com.nimbo.analyticsquery.intent.model.QueryRequirements;
import com.nimbo.analyticsquery.model.AnalysisDataSource;
import java.util.HashSet;
import java.util.Set;

public final class SqlPolicyRules {

  private static final Set<String> COMMON_FUNCTIONS =
      Set.of(
          "ABS",
          "AVG",
          "CEIL",
          "CEILING",
          "COALESCE",
          "COUNT",
          "FLOOR",
          "GREATEST",
          "LEAST",
          "LOWER",
          "MAX",
          "MIN",
          "NULLIF",
          "ROUND",
          "SUM",
          "UPPER");

  private static final Set<String> POSTGRESQL_FUNCTIONS = Set.of("DATE_BIN", "DATE_TRUNC");

  private static final Set<String> STARROCKS_FUNCTIONS =
      Set.of("DATE_TRUNC", "FROM_UNIXTIME", "STR_TO_DATE", "TO_DATE");

  private SqlPolicyRules() {}

  public static Set<String> allowedFunctions(AnalysisDataSource dataSource) {

    Set<String> allowed = new HashSet<>(COMMON_FUNCTIONS);

    allowed.addAll(
        switch (dataSource) {
          case POSTGRESQL -> POSTGRESQL_FUNCTIONS;
          case STARROCKS -> STARROCKS_FUNCTIONS;
        });

    return Set.copyOf(allowed);
  }

  public static int maxResultRows() {
    return QueryRequirements.MAX_LIMIT;
  }
}
