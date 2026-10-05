package com.nimbo.analyticsquery.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.nimbo.analyticsquery.intent.model.QueryRequirements;
import com.nimbo.analyticsquery.intent.model.QueryRequirements.Aggregation;
import com.nimbo.analyticsquery.model.AnalysisDataSource;
import com.nimbo.analyticsquery.routing.DataSourceRoute;
import com.nimbo.analyticsquery.schema.PostgresSemanticCatalogSource;
import com.nimbo.analyticsquery.schema.SemanticColumn;
import com.nimbo.analyticsquery.schema.SemanticFieldNames;
import com.nimbo.analyticsquery.schema.SemanticTable;

class SqlGenerationContextFactoryTest {

    private final SqlGenerationContextFactory factory = new SqlGenerationContextFactory();

    @Test
    void givenPostgresRouteWhenContextIsCreatedThenOnlyRoutedAnalyticsSchemaIsExposed() {
        // Given: a deterministic route to the PostgreSQL traffic cube
        SemanticTable traffic =
                new PostgresSemanticCatalogSource()
                        .tables().stream()
                                .filter(table -> table.name().equals("traffic_aggregates"))
                                .findFirst()
                                .orElseThrow();
        DataSourceRoute route = new DataSourceRoute(AnalysisDataSource.POSTGRESQL, traffic);

        // When: the SQL generation context is created
        SqlGenerationContext context = factory.create(route);

        // Then: only the routed table and query-safe columns are exposed
        assertThat(context.dataSource()).isEqualTo(AnalysisDataSource.POSTGRESQL);
        assertThat(context.tables())
                .extracting(SqlGenerationContext.Table::name)
                .containsExactly("traffic_aggregates");

        SqlGenerationContext.Table table = context.tables().get(0);

        assertThat(table.columns())
                .extracting(SemanticColumn::name)
                .contains("src_ip", "window_start", "window_end", "in_bytes")
                .doesNotContain("batch_id", "ingested_at");
        assertThat(table.timeSemantics().column()).isEqualTo("window_start");
        assertThat(table.timeSemantics().timeZone()).isEqualTo(ZoneOffset.UTC);
        assertThat(table.timeSemantics().resolution()).isEqualTo(Duration.ofHours(1));
        assertThat(table.semanticExpressions())
                .contains(
                        new SqlGenerationContext.SemanticExpression(
                                SemanticFieldNames.TOTAL_BYTES,
                                "(in_bytes + out_bytes)"),
                        new SqlGenerationContext.SemanticExpression(
                                SemanticFieldNames.TOTAL_PACKETS,
                                "(in_packets + out_packets)"));
        assertThat(context.queryRestrictions())
                .contains(
                        "Do not reference another datasource.",
                        "Always use a literal LIMIT between 1 and "
                                + QueryRequirements.MAX_LIMIT
                                + " rows.");
    }

    @Test
    void givenStarRocksRouteWhenContextIsCreatedThenOnlyStarRocksTableMetadataIsUsed() {
        // Given: a routed StarRocks table with analytics and operational columns
        SemanticTable netflow =
                new SemanticTable(
                        "nimbo.netflow",
                        AnalysisDataSource.STARROCKS,
                        "Raw NetFlow records.",
                        "one NetFlow record",
                        "process_time",
                        null,
                        List.of(
                                column(
                                        "process_time",
                                        "BIGINT",
                                        "Kafka record timestamp in epoch milliseconds.",
                                        Set.of(SemanticColumn.Role.TIME),
                                        null,
                                        Set.of()),
                                column(
                                        "ipv4_src_addr",
                                        "VARCHAR(15)",
                                        "Source IPv4 address.",
                                        Set.of(SemanticColumn.Role.DIMENSION),
                                        SemanticFieldNames.SOURCE_IP,
                                        Set.of(Aggregation.COUNT_DISTINCT)),
                                column(
                                        "kafka_offset",
                                        "BIGINT",
                                        "Kafka record offset.",
                                        Set.of(SemanticColumn.Role.METADATA),
                                        null,
                                        Set.of())),
                        List.of("Contains raw NetFlow dimensions."),
                        List.of("Source-IP analysis."),
                        List.of("Cross-datasource joins."));
        DataSourceRoute route = new DataSourceRoute(AnalysisDataSource.STARROCKS, netflow);

        // When: the SQL generation context is created
        SqlGenerationContext context = factory.create(route);

        // Then: the selected datasource and approved analytical columns are retained
        assertThat(context.dataSource()).isEqualTo(AnalysisDataSource.STARROCKS);
        assertThat(context.tables())
                .singleElement()
                .satisfies(
                        table -> {
                            assertThat(table.name()).isEqualTo("nimbo.netflow");
                            assertThat(table.columns())
                                    .extracting(SemanticColumn::name)
                                    .containsExactly("process_time", "ipv4_src_addr");
                            assertThat(table.columns())
                                    .filteredOn(column -> column.name().equals("process_time"))
                                    .singleElement()
                                    .extracting(SemanticColumn::meaning)
                                    .asString()
                                    .contains("epoch milliseconds");
                        });
    }

    private static SemanticColumn column(
            String name,
            String type,
            String meaning,
            Set<SemanticColumn.Role> roles,
            String semanticField,
            Set<Aggregation> allowedAggregations) {

        return new SemanticColumn(
                name, type, meaning, roles, false, semanticField, allowedAggregations);
    }
}
