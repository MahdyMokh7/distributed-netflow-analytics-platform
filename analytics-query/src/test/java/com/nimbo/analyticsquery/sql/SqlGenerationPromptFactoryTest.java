package com.nimbo.analyticsquery.sql;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.nimbo.analyticsquery.config.SqlGenerationProperties;
import com.nimbo.analyticsquery.intent.model.QueryRequirements;
import com.nimbo.analyticsquery.model.AnalysisDataSource;
import com.nimbo.analyticsquery.schema.SemanticColumn;
import com.nimbo.analyticsquery.schema.SemanticFieldNames;

class SqlGenerationPromptFactoryTest {

    private final SqlGenerationPromptFactory factory =
            new SqlGenerationPromptFactory(
                    new ObjectMapper().findAndRegisterModules(),
                    new SqlGenerationProperties(4_096));

    @Test
    void givenPostgresContextWhenPromptIsCreatedThenStarRocksSchemaIsNotExposed() {
        // Given: a context containing only the routed PostgreSQL table
        SqlGenerationContext context = context(
                AnalysisDataSource.POSTGRESQL,
                "traffic_aggregates",
                "src_ip",
                SemanticFieldNames.SOURCE_IP);

        // When: the SQL generation system prompt is created
        String prompt = factory.createSystemPrompt(QueryRequirements.empty(), context);

        // Then: only the PostgreSQL schema is visible to the model
        assertThat(prompt)
                .contains("POSTGRESQL", "traffic_aggregates", "src_ip")
                .doesNotContain("STARROCKS", "nimbo.netflow", "ipv4_src_addr");
    }

    @Test
    void givenStarRocksContextWhenPromptIsCreatedThenPostgresSchemaIsNotExposed() {
        // Given: a context containing only the routed StarRocks table
        SqlGenerationContext context = context(
                AnalysisDataSource.STARROCKS,
                "nimbo.netflow",
                "ipv4_src_addr",
                SemanticFieldNames.SOURCE_IP);

        // When: the SQL generation system prompt is created
        String prompt = factory.createSystemPrompt(QueryRequirements.empty(), context);

        // Then: only the StarRocks schema is visible to the model
        assertThat(prompt)
                .contains("STARROCKS", "nimbo.netflow", "ipv4_src_addr")
                .doesNotContain("POSTGRESQL", "traffic_aggregates", "src_ip");
    }

    private static SqlGenerationContext context(
            AnalysisDataSource dataSource,
            String tableName,
            String columnName,
            String semanticField) {

        SemanticColumn column =
                new SemanticColumn(
                        columnName,
                        "VARCHAR(15)",
                        "Source IPv4 address.",
                        Set.of(SemanticColumn.Role.DIMENSION),
                        false,
                        semanticField,
                        Set.of(QueryRequirements.Aggregation.COUNT_DISTINCT));

        return new SqlGenerationContext(
                dataSource,
                List.of(
                        new SqlGenerationContext.Table(
                                tableName,
                                "NetFlow analytics data.",
                                "one source IP and time window",
                                new SqlGenerationContext.TimeSemantics(
                                        columnName,
                                        ZoneOffset.UTC,
                                        Duration.ofHours(1)),
                                List.of(column),
                                List.of())),
                List.of("Use only the listed table and columns."));
    }
}
