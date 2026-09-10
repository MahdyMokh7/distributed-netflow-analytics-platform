package com.nimbo.batch.processor;

import com.nimbo.batch.exception.BatchProcessingException;
import com.nimbo.batch.support.PostgresContainerExtension;
import com.nimbo.batch.support.ProcessorTestFixture;
import com.nimbo.batch.support.SparkSessionExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class TrafficAggregationProcessorIT {

    @RegisterExtension
    static final PostgresContainerExtension POSTGRES = new PostgresContainerExtension();

    @RegisterExtension
    static final SparkSessionExtension SPARK = new SparkSessionExtension();

    private ProcessorTestFixture fixture;

    @BeforeEach
    void setUp() {
        fixture = new ProcessorTestFixture(POSTGRES, SPARK.spark());
        fixture.resetDatabase();
    }

    @Test
    void givenFlowsInTwoWindows_whenProcessed_thenTrafficTotalsArePersisted() {
        // Given: deterministic flows with repeated and distinct source addresses
        TrafficAggregationProcessor processor = new TrafficAggregationProcessor(
                fixture.batchConfig(), fixture.databaseOutput(), fixture.postgresWriter());
        UUID batchId = UUID.fromString("10000000-0000-0000-0000-000000000001");

        // When: traffic aggregation writes database output
        processor.process(fixture.standardFlows(), batchId);

        // Then: source-window totals exactly match the input records
        assertThat(fixture.countRows("traffic_aggregates")).isEqualTo(2);
        assertThat(fixture.queryLong("SELECT in_bytes FROM traffic_aggregates WHERE src_ip = '10.0.0.1'"))
                .isEqualTo(450L);
        assertThat(fixture.queryLong("SELECT out_bytes FROM traffic_aggregates WHERE src_ip = '10.0.0.1'"))
                .isEqualTo(670L);
        assertThat(fixture.queryLong("SELECT record_count FROM traffic_aggregates WHERE src_ip = '10.0.0.1'"))
                .isEqualTo(3L);
    }

    @Test
    void givenTheSameBatchTwice_whenProcessed_thenTrafficUpsertRemainsExactlyOnce() {
        // Given: one fixed batch identity and deterministic flow records
        TrafficAggregationProcessor processor = new TrafficAggregationProcessor(
                fixture.batchConfig(), fixture.databaseOutput(), fixture.postgresWriter());
        UUID batchId = UUID.fromString("10000000-0000-0000-0000-000000000002");

        // When: the same batch is processed twice
        processor.process(fixture.standardFlows(), batchId);
        processor.process(fixture.standardFlows(), batchId);

        // Then: primary-key upserts preserve two source-window rows
        assertThat(fixture.countRows("traffic_aggregates")).isEqualTo(2);
        assertThat(fixture.queryLong("SELECT SUM(record_count) FROM traffic_aggregates")).isEqualTo(4L);
    }

    @Test
    void givenNoFlows_whenProcessed_thenNoTrafficRowsAreWritten() {
        // Given: an empty deterministic NetFlow dataset
        TrafficAggregationProcessor processor = new TrafficAggregationProcessor(
                fixture.batchConfig(), fixture.databaseOutput(), fixture.postgresWriter());

        // When: traffic aggregation processes the empty dataset
        processor.process(
                fixture.emptyFlows(), UUID.fromString("10000000-0000-0000-0000-000000000003"));

        // Then: no traffic rows are persisted
        assertThat(fixture.countRows("traffic_aggregates")).isZero();
    }

    @Test
    void givenFlowsMissingRequiredColumns_whenProcessed_thenBatchFailureIsReported() {
        // Given: a dataset without required traffic aggregation columns
        TrafficAggregationProcessor processor = new TrafficAggregationProcessor(
                fixture.batchConfig(), fixture.databaseOutput(), fixture.postgresWriter());

        // When: traffic aggregation is invoked with the incomplete schema
        var invocation = org.assertj.core.api.Assertions.catchThrowable(() -> processor.process(
                fixture.flowsMissingRequiredColumns(),
                UUID.fromString("10000000-0000-0000-0000-000000000004")));

        // Then: the schema error is wrapped with processor context
        assertThat(invocation)
                .isInstanceOf(BatchProcessingException.class)
                .hasMessageContaining("Traffic aggregation failed");
    }
}
