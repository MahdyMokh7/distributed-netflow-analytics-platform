package com.nimbo.analyticsquery.sql;

import com.nimbo.analyticsquery.model.AnalysisDataSource;
import com.nimbo.analyticsquery.routing.DataSourceRoute;
import com.nimbo.analyticsquery.schema.SemanticColumn;
import com.nimbo.analyticsquery.schema.SemanticTable;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import org.springframework.stereotype.Component;

@Component
public class SqlGenerationContextFactory {

  private static final List<String> QUERY_RESTRICTIONS =
      List.of(
          "Generate exactly one read-only SELECT statement.",
          "Use only the listed table and columns.",
          "Do not access system catalogs or discover schema.",
          "Do not reference another datasource.",
          "Use the listed time column for the bounded time range.",
          "Use only relationships explicitly listed in the context.",
          "Always use a literal LIMIT between 1 and " + SqlPolicyRules.maxResultRows() + " rows.");

  private static final String POSTGRESQL_BATCH_AGGREGATION_RESTRICTION =
      "PostgreSQL cube rows may contain batch fragments for the same logical result. "
          + "Never expose or group by batch_id. Sum additive metrics across all fragments at the "
          + "requested output grain; for per-window results, group by the table's complete logical "
          + "window grain.";

  private static final Set<SemanticColumn.Role> EXPOSED_ROLES =
      Set.of(SemanticColumn.Role.DIMENSION, SemanticColumn.Role.METRIC, SemanticColumn.Role.TIME);

  public SqlGenerationContext create(DataSourceRoute route) {
    Objects.requireNonNull(route, "route must not be null");

    SemanticTable table = route.table();

    return new SqlGenerationContext(
        route.dataSource(), List.of(table(table)), queryRestrictions(route.dataSource()));
  }

  private List<String> queryRestrictions(AnalysisDataSource dataSource) {
    if (dataSource != AnalysisDataSource.POSTGRESQL) {
      return QUERY_RESTRICTIONS;
    }

    return Stream.concat(
            QUERY_RESTRICTIONS.stream(), Stream.of(POSTGRESQL_BATCH_AGGREGATION_RESTRICTION))
        .toList();
  }

  private SqlGenerationContext.Table table(SemanticTable table) {

    List<SemanticColumn> columns = table.columns().stream().filter(this::isExposed).toList();

    SemanticColumn timeColumn =
        table
            .column(table.timestampColumn())
            .orElseThrow(() -> new IllegalStateException("Routed table has no timestamp column"));

    return new SqlGenerationContext.Table(
        table.name(),
        table.meaning(),
        table.grain(),
        new SqlGenerationContext.TimeSemantics(
            timeColumn.name(), ZoneOffset.UTC, table.timeResolution()),
        columns,
        table.relationships());
  }

  private boolean isExposed(SemanticColumn column) {
    return column.roles().stream().anyMatch(EXPOSED_ROLES::contains);
  }
}
