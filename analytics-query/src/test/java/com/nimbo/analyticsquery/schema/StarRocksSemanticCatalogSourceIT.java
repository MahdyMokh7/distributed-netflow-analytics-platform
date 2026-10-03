package com.nimbo.analyticsquery.schema;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.jdbc.core.JdbcTemplate;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import com.nimbo.analyticsquery.config.StarRocksCatalogProperties;
import com.nimbo.analyticsquery.intent.model.QueryRequirements;
import com.nimbo.analyticsquery.model.AnalysisDataSource;
import com.nimbo.analyticsquery.planner.SqlFallbackEligibility;
import com.nimbo.analyticsquery.planner.SqlFallbackEligibilityPolicy;
import com.nimbo.analyticsquery.routing.DataSourceRoute;
import com.nimbo.analyticsquery.routing.DataSourceRouter;
import com.nimbo.analyticsquery.support.StarRocksCatalogTestExtension;

@ExtendWith(StarRocksCatalogTestExtension.class)
class StarRocksSemanticCatalogSourceIT {

    @Test
    void givenCurrentPhysicalSchemaWhenRefreshedThenSemanticTableUsesDiscoveredMetadata(
            StarRocksCatalogTestExtension starRocks) {
        // Given: a live StarRocks table containing a newly named metric
        try (HikariDataSource dataSource = dataSource(starRocks)) {
            StarRocksSemanticCatalogSource source = source(dataSource, starRocks);

            // When: the semantic catalog is refreshed
            boolean refreshed = source.refresh();

            // Then: physical type, nullability, comment and inferred role are published
            SemanticTable table = source.tables().iterator().next();
            SemanticColumn column = table.column("new_flow_bytes").orElseThrow();
            assertThat(refreshed).isTrue();
            assertThat(column.sqlType()).startsWithIgnoringCase("bigint");
            assertThat(column.nullable()).isTrue();
            assertThat(column.meaning()).isEqualTo("New flow byte count");
            assertThat(column.roles()).containsExactly(SemanticColumn.Role.METRIC);
            assertThat(column.semanticField()).isEqualTo("NEW_FLOW_BYTES");
            assertThat(table.timeResolution()).isNull();
            CuratedSemanticSchemaCatalog catalog =
                    new CuratedSemanticSchemaCatalog(List.of(source));
            assertThat(catalog.find("NEW_FLOW_BYTES")).isPresent();
            assertThat(catalog.find("MIN_TTL").orElseThrow().allowedAggregations())
                    .doesNotContain(QueryRequirements.Aggregation.SUM);
        }
    }

    @Test
    void givenPublishedSnapshotWhenRefreshFailsThenLastKnownGoodSnapshotRemains(
            StarRocksCatalogTestExtension starRocks) throws Exception {
        // Given: a successful catalog snapshot followed by a missing physical table
        try (HikariDataSource dataSource = dataSource(starRocks)) {
            StarRocksSemanticCatalogSource source = source(dataSource, starRocks);
            assertThat(source.refresh()).isTrue();
            List<SemanticTable> previous = List.copyOf(source.tables());
            starRocks.dropTable();

            // When: the next catalog refresh fails
            boolean refreshed = source.refresh();

            // Then: the previous immutable snapshot remains available
            assertThat(refreshed).isFalse();
            assertThat(source.tables()).containsExactlyElementsOf(previous);
        }
    }

    @Test
    void givenRequirementsSplitAcrossDatasourcesWhenEvaluatedThenFallbackIsDenied(
            StarRocksCatalogTestExtension starRocks) {
        // Given: one request requiring PostgreSQL and live StarRocks-only fields
        try (HikariDataSource dataSource = dataSource(starRocks)) {
            StarRocksSemanticCatalogSource starRocksSource = source(dataSource, starRocks);
            assertThat(starRocksSource.refresh()).isTrue();
            CuratedSemanticSchemaCatalog catalog =
                    new CuratedSemanticSchemaCatalog(
                            List.of(new PostgresSemanticCatalogSource(), starRocksSource));
            QueryRequirements requirements =
                    new QueryRequirements(
                            List.of(),
                            List.of(
                                    metric(SemanticFieldNames.RECORD_COUNT),
                                    metric("NEW_FLOW_BYTES")),
                            List.of(),
                            List.of(),
                            QueryRequirements.TrafficDirection.UNSPECIFIED,
                            new QueryRequirements.TimeRange(
                                    Instant.parse("2026-10-01T08:00:00Z"),
                                    Instant.parse("2026-10-01T09:00:00Z")),
                            List.of(),
                            null,
                            List.of(),
                            null,
                            false);

            // When: SQL fallback eligibility is evaluated
            SqlFallbackEligibility result =
                    new SqlFallbackEligibilityPolicy(catalog).evaluate(requirements);

            // Then: cross-datasource SQL is rejected
            assertThat(result.eligible()).isFalse();
            assertThat(result.message()).contains("multiple physical datasources");
        }
    }

    @Test
    void givenBothDatasourcesPreserveSemanticsWhenRoutedThenPostgresIsPreferred(
            StarRocksCatalogTestExtension starRocks) {
        // Given: source-byte requirements supported by a cube and raw records
        try (HikariDataSource dataSource = dataSource(starRocks)) {
            CuratedSemanticSchemaCatalog catalog = catalog(dataSource, starRocks);
            QueryRequirements requirements =
                    requirements(
                            List.of(SemanticFieldNames.SOURCE_IP),
                            SemanticFieldNames.IN_BYTES,
                            QueryRequirements.Aggregation.SUM,
                            Duration.ofHours(1));

            // When: the requirements are routed
            Optional<DataSourceRoute> route = new DataSourceRouter(catalog).route(requirements);

            // Then: the valid PostgreSQL aggregate is preferred
            assertThat(route).isPresent();
            assertThat(route.orElseThrow().dataSource()).isEqualTo(AnalysisDataSource.POSTGRESQL);
            assertThat(route.orElseThrow().table().name()).isEqualTo("traffic_aggregates");
        }
    }

    @Test
    void givenFinerGranularityWhenRoutedThenStarRocksIsSelected(
            StarRocksCatalogTestExtension starRocks) {
        // Given: source-byte requirements finer than the PostgreSQL cube
        try (HikariDataSource dataSource = dataSource(starRocks)) {
            CuratedSemanticSchemaCatalog catalog = catalog(dataSource, starRocks);
            QueryRequirements requirements =
                    requirements(
                            List.of(SemanticFieldNames.SOURCE_IP),
                            SemanticFieldNames.IN_BYTES,
                            QueryRequirements.Aggregation.SUM,
                            Duration.ofMinutes(15));

            // When: the requirements are routed
            Optional<DataSourceRoute> route = new DataSourceRouter(catalog).route(requirements);

            // Then: current raw StarRocks data preserves the finer grain
            assertThat(route).isPresent();
            assertThat(route.orElseThrow().dataSource()).isEqualTo(AnalysisDataSource.STARROCKS);
            assertThat(route.orElseThrow().table().name()).isEqualTo("nimbo.netflow");
        }
    }

    @Test
    void givenNewApprovedStarRocksFieldsWhenRoutedThenCurrentSnapshotIsUsed(
            StarRocksCatalogTestExtension starRocks) {
        // Given: requirements using fields discovered from the live schema
        try (HikariDataSource dataSource = dataSource(starRocks)) {
            CuratedSemanticSchemaCatalog catalog = catalog(dataSource, starRocks);
            QueryRequirements requirements =
                    requirements(
                            List.of("DNS_QUERY_TYPE"),
                            "NEW_FLOW_BYTES",
                            QueryRequirements.Aggregation.AVG,
                            null);

            // When: the requirements are routed
            Optional<DataSourceRoute> route = new DataSourceRouter(catalog).route(requirements);

            // Then: the refreshed StarRocks table is selected
            assertThat(route).isPresent();
            assertThat(route.orElseThrow().dataSource()).isEqualTo(AnalysisDataSource.STARROCKS);
        }
    }

    @Test
    void givenMeasurementSumWhenRoutedThenStarRocksIsNotSelected(
            StarRocksCatalogTestExtension starRocks) {
        // Given: a semantically invalid sum of minimum TTL measurements
        try (HikariDataSource dataSource = dataSource(starRocks)) {
            CuratedSemanticSchemaCatalog catalog = catalog(dataSource, starRocks);
            QueryRequirements requirements =
                    requirements(List.of(), "MIN_TTL", QueryRequirements.Aggregation.SUM, null);

            // When: the requirements are routed
            Optional<DataSourceRoute> route = new DataSourceRouter(catalog).route(requirements);

            // Then: physical availability does not permit an invalid aggregation
            assertThat(route).isEmpty();
        }
    }

    @Test
    void givenMeasurementAverageWhenRoutedThenStarRocksIsSelected(
            StarRocksCatalogTestExtension starRocks) {
        // Given: a supported average over minimum TTL measurements
        try (HikariDataSource dataSource = dataSource(starRocks)) {
            CuratedSemanticSchemaCatalog catalog = catalog(dataSource, starRocks);
            QueryRequirements requirements =
                    requirements(List.of(), "MIN_TTL", QueryRequirements.Aggregation.AVG, null);

            // When: the requirements are routed
            Optional<DataSourceRoute> route = new DataSourceRouter(catalog).route(requirements);

            // Then: StarRocks is selected for the valid measurement aggregation
            assertThat(route).isPresent();
            assertThat(route.orElseThrow().dataSource()).isEqualTo(AnalysisDataSource.STARROCKS);
        }
    }

    private static StarRocksSemanticCatalogSource source(
            HikariDataSource dataSource, StarRocksCatalogTestExtension starRocks) {

        StarRocksCatalogProperties properties =
                new StarRocksCatalogProperties(
                        true,
                        starRocks.jdbcUrl(),
                        "root",
                        "",
                        "nimbo",
                        "netflow",
                        Duration.ofMinutes(15),
                        Duration.ofSeconds(5),
                        1,
                        10);

        return new StarRocksSemanticCatalogSource(new JdbcTemplate(dataSource), properties);
    }

    private static HikariDataSource dataSource(StarRocksCatalogTestExtension starRocks) {

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(starRocks.jdbcUrl());
        config.setUsername("root");
        config.setPassword("");
        config.setMaximumPoolSize(1);

        return new HikariDataSource(config);
    }

    private static CuratedSemanticSchemaCatalog catalog(
            HikariDataSource dataSource, StarRocksCatalogTestExtension starRocks) {

        StarRocksSemanticCatalogSource starRocksSource = source(dataSource, starRocks);
        assertThat(starRocksSource.refresh()).isTrue();

        return new CuratedSemanticSchemaCatalog(
                List.of(new PostgresSemanticCatalogSource(), starRocksSource));
    }

    private static QueryRequirements requirements(
            List<String> dimensions,
            String metric,
            QueryRequirements.Aggregation aggregation,
            Duration granularity) {

        return new QueryRequirements(
                dimensions,
                List.of(new QueryRequirements.MetricRequirement(metric, aggregation)),
                List.of(),
                List.of(),
                QueryRequirements.TrafficDirection.UNSPECIFIED,
                new QueryRequirements.TimeRange(
                        Instant.parse("2026-10-01T08:00:00Z"),
                        Instant.parse("2026-10-01T09:00:00Z")),
                dimensions,
                granularity,
                List.of(),
                null,
                false);
    }

    private static QueryRequirements.MetricRequirement metric(String field) {

        return new QueryRequirements.MetricRequirement(field, QueryRequirements.Aggregation.SUM);
    }
}
