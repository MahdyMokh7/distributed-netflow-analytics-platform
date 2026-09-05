package com.nimbo.batch.processor;

import com.nimbo.batch.exception.BatchProcessingException;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TrafficAggregationProcessorTest extends BaseProcessorTest {

    @Test
    void givenFlowsInTwoWindows_whenProcessed_thenTrafficTotalsArePersisted() {
        // Given: deterministic flows with repeated and distinct source addresses
        TrafficAggregationProcessor processor = new TrafficAggregationProcessor(batchConfig(), databaseOutput(), postgresWriter());
        UUID batchId = UUID.fromString("10000000-0000-0000-0000-000000000001");

        // When: traffic aggregation writes the database output
        processor.process(standardFlows(), batchId);

        // Then: source-window totals match the expected aggregation
        assertThat(countRows("traffic_aggregates")).isEqualTo(2);
        assertThat(queryLong("SELECT in_bytes FROM traffic_aggregates WHERE src_ip = '10.0.0.1'")).isEqualTo(450L);
        assertThat(queryLong("SELECT out_bytes FROM traffic_aggregates WHERE src_ip = '10.0.0.1'")).isEqualTo(670L);
        assertThat(queryLong("SELECT record_count FROM traffic_aggregates WHERE src_ip = '10.0.0.1'")).isEqualTo(3L);
    }

    @Test
    void givenTheSameBatchTwice_whenProcessed_thenTrafficUpsertRemainsExactlyOnce() {
        // Given: one fixed batch identifier and deterministic flow records
        TrafficAggregationProcessor processor = new TrafficAggregationProcessor(batchConfig(), databaseOutput(), postgresWriter());
        UUID batchId = UUID.fromString("10000000-0000-0000-0000-000000000002");

        // When: the same batch is processed twice
        processor.process(standardFlows(), batchId);
        processor.process(standardFlows(), batchId);

        // Then: primary-key upserts preserve two source-window rows
        assertThat(countRows("traffic_aggregates")).isEqualTo(2);
        assertThat(queryLong("SELECT SUM(record_count) FROM traffic_aggregates")).isEqualTo(4L);
    }

    @Test
    void givenNoFlows_whenProcessed_thenNoTrafficRowsAreWritten() {
        // Given: an empty netflow dataset
        TrafficAggregationProcessor processor = new TrafficAggregationProcessor(batchConfig(), databaseOutput(), postgresWriter());

        // When: traffic aggregation runs
        processor.process(emptyFlows(), UUID.fromString("10000000-0000-0000-0000-000000000003"));

        // Then: no database rows are created
        assertThat(countRows("traffic_aggregates")).isZero();
    }

    @Test
    void givenFlowsMissingRequiredColumns_whenProcessed_thenBatchFailureIsReported() {
        // Given: a dataset without traffic aggregation columns
        TrafficAggregationProcessor processor = new TrafficAggregationProcessor(batchConfig(), databaseOutput(), postgresWriter());

        // When: traffic aggregation is invoked
        // Then: the source-schema error is wrapped with batch context
        assertThatThrownBy(() -> processor.process(flowsMissingRequiredColumns(), UUID.fromString("10000000-0000-0000-0000-000000000004")))
                .isInstanceOf(BatchProcessingException.class)
                .hasMessageContaining("Traffic aggregation failed");
    }
}
