package com.nimbo.analyticsquery.sql;

import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.springframework.stereotype.Component;

import com.nimbo.analyticsquery.routing.DataSourceRoute;
import com.nimbo.analyticsquery.schema.SemanticColumn;
import com.nimbo.analyticsquery.schema.SemanticTable;

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

    private static final Set<SemanticColumn.Role> EXPOSED_ROLES =
            Set.of(
                    SemanticColumn.Role.DIMENSION,
                    SemanticColumn.Role.METRIC,
                    SemanticColumn.Role.TIME);

    public SqlGenerationContext create(DataSourceRoute route) {
        Objects.requireNonNull(route, "route must not be null");

        SemanticTable table = route.table();

        return new SqlGenerationContext(
                route.dataSource(), List.of(table(table)), QUERY_RESTRICTIONS);
    }

    private SqlGenerationContext.Table table(SemanticTable table) {

        List<SemanticColumn> columns = table.columns().stream().filter(this::isExposed).toList();

        SemanticColumn timeColumn =
                table.column(table.timestampColumn())
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "Routed table has no timestamp column"));

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
