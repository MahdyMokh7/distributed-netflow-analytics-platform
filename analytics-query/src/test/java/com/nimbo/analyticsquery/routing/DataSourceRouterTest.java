package com.nimbo.analyticsquery.routing;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.nimbo.analyticsquery.intent.model.QueryRequirements;
import com.nimbo.analyticsquery.model.AnalysisDataSource;
import com.nimbo.analyticsquery.schema.CuratedSemanticSchemaCatalog;
import com.nimbo.analyticsquery.schema.PostgresSemanticCatalogSource;
import com.nimbo.analyticsquery.schema.SemanticFieldNames;

class DataSourceRouterTest {

    private static final Instant START = Instant.parse("2026-10-01T08:00:00Z");
    private static final Instant END = Instant.parse("2026-10-01T09:00:00Z");

    private final DataSourceRouter router =
            new DataSourceRouter(
                    new CuratedSemanticSchemaCatalog(List.of(new PostgresSemanticCatalogSource())));

    @Test
    void givenSourceTrafficRequirementsWhenRoutedThenTrafficCubeIsSelected() {
        // Given: additive source-IP requirements aligned to the cube window
        QueryRequirements requirements =
                requirements(
                        List.of(SemanticFieldNames.SOURCE_IP),
                        List.of(
                                metric(
                                        SemanticFieldNames.IN_BYTES,
                                        QueryRequirements.Aggregation.SUM)),
                        START,
                        END,
                        Duration.ofHours(1));

        // When: the requirements are routed
        Optional<DataSourceRoute> route = router.route(requirements);

        // Then: the most specific PostgreSQL cube is selected
        assertThat(route).isPresent();
        assertThat(route.orElseThrow().dataSource()).isEqualTo(AnalysisDataSource.POSTGRESQL);
        assertThat(route.orElseThrow().table().name()).isEqualTo("traffic_aggregates");
    }

    @Test
    void givenConnectionPairRequirementsWhenRoutedThenConnectionCubeIsSelected() {
        // Given: additive source/destination pair requirements
        QueryRequirements requirements =
                requirements(
                        List.of(SemanticFieldNames.SOURCE_IP, SemanticFieldNames.DESTINATION_IP),
                        List.of(
                                metric(
                                        SemanticFieldNames.OUT_PACKETS,
                                        QueryRequirements.Aggregation.SUM)),
                        START,
                        END,
                        null);

        // When: the requirements are routed
        Optional<DataSourceRoute> route = router.route(requirements);

        // Then: the pair-grain PostgreSQL cube is selected
        assertThat(route).isPresent();
        assertThat(route.orElseThrow().table().name()).isEqualTo("connection_aggregates");
    }

    @Test
    void givenSubHourGranularityWhenRoutedThenPostgresIsNotSelected() {
        // Given: a request finer than the one-hour aggregate cubes
        QueryRequirements requirements =
                requirements(
                        List.of(SemanticFieldNames.SOURCE_IP),
                        List.of(
                                metric(
                                        SemanticFieldNames.IN_BYTES,
                                        QueryRequirements.Aggregation.SUM)),
                        START,
                        END,
                        Duration.ofMinutes(15));

        // When: the requirements are routed without StarRocks available
        Optional<DataSourceRoute> route = router.route(requirements);

        // Then: no semantically incorrect cube route is returned
        assertThat(route).isEmpty();
    }

    @Test
    void givenUnalignedRangeWhenRoutedThenPostgresIsNotSelected() {
        // Given: a range that would include partial one-hour cube windows
        QueryRequirements requirements =
                requirements(
                        List.of(SemanticFieldNames.SOURCE_IP),
                        List.of(
                                metric(
                                        SemanticFieldNames.IN_BYTES,
                                        QueryRequirements.Aggregation.SUM)),
                        START.plus(Duration.ofMinutes(15)),
                        END.plus(Duration.ofMinutes(15)),
                        null);

        // When: the requirements are routed without StarRocks available
        Optional<DataSourceRoute> route = router.route(requirements);

        // Then: PostgreSQL is rejected because it cannot preserve the range
        assertThat(route).isEmpty();
    }

    @Test
    void givenUnsafeCubeAverageWhenRoutedThenPostgresIsNotSelected() {
        // Given: an average that cannot be recovered from batch-level sums
        QueryRequirements requirements =
                requirements(
                        List.of(SemanticFieldNames.SOURCE_IP),
                        List.of(
                                metric(
                                        SemanticFieldNames.IN_BYTES,
                                        QueryRequirements.Aggregation.AVG)),
                        START,
                        END,
                        null);

        // When: the requirements are routed without raw records available
        Optional<DataSourceRoute> route = router.route(requirements);

        // Then: the aggregate cube is not treated as semantically sufficient
        assertThat(route).isEmpty();
    }

    @Test
    void givenRequirementsSplitAcrossCubesWhenRoutedThenNoTableIsSelected() {
        // Given: destination IP and record count exist only in separate cubes
        QueryRequirements requirements =
                requirements(
                        List.of(SemanticFieldNames.DESTINATION_IP),
                        List.of(
                                metric(
                                        SemanticFieldNames.RECORD_COUNT,
                                        QueryRequirements.Aggregation.SUM)),
                        START,
                        END,
                        null);

        // When: the requirements are routed
        Optional<DataSourceRoute> route = router.route(requirements);

        // Then: fields are not combined across semantic tables
        assertThat(route).isEmpty();
    }

    private static QueryRequirements requirements(
            List<String> dimensions,
            List<QueryRequirements.MetricRequirement> metrics,
            Instant start,
            Instant end,
            Duration granularity) {

        return new QueryRequirements(
                dimensions,
                metrics,
                List.of(),
                List.of(),
                QueryRequirements.TrafficDirection.UNSPECIFIED,
                new QueryRequirements.TimeRange(start, end),
                dimensions,
                granularity,
                List.of(),
                null,
                false);
    }

    private static QueryRequirements.MetricRequirement metric(
            String field, QueryRequirements.Aggregation aggregation) {

        return new QueryRequirements.MetricRequirement(field, aggregation);
    }
}
