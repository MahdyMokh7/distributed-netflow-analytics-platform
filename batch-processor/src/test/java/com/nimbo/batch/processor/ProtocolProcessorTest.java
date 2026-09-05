package com.nimbo.batch.processor;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ProtocolProcessorTest extends BaseProcessorTest {

    @Test
    void givenFlowsAcrossProtocolsAndWindows_whenProcessed_thenProtocolUsageIsPersisted() {
        // Given: deterministic TCP and UDP flows in two windows
        ProtocolProcessor processor = new ProtocolProcessor(batchConfig(), databaseOutput(), postgresWriter());
        UUID batchId = UUID.fromString("40000000-0000-0000-0000-000000000001");

        // When: protocol aggregation writes database output
        processor.process(standardFlows(), batchId);

        // Then: protocol-window metrics match the input flows
        assertThat(countRows("protocol_usage_aggregates")).isEqualTo(3);
        assertThat(queryLong("SELECT connection_count FROM protocol_usage_aggregates WHERE l7_protocol = 6.0 AND in_bytes = 400")).isEqualTo(2L);
        assertThat(queryLong("SELECT out_bytes FROM protocol_usage_aggregates WHERE l7_protocol = 6.0 AND in_bytes = 400")).isEqualTo(600L);
    }

    @Test
    void givenTheSameBatchTwice_whenProcessed_thenProtocolUpsertRemainsExactlyOnce() {
        // Given: one fixed batch identifier and deterministic flow records
        ProtocolProcessor processor = new ProtocolProcessor(batchConfig(), databaseOutput(), postgresWriter());
        UUID batchId = UUID.fromString("40000000-0000-0000-0000-000000000002");

        // When: the same batch is processed twice
        processor.process(standardFlows(), batchId);
        processor.process(standardFlows(), batchId);

        // Then: the three protocol-window rows are not duplicated
        assertThat(countRows("protocol_usage_aggregates")).isEqualTo(3);
    }

    @Test
    void givenNoFlows_whenProcessed_thenNoProtocolRowsAreWritten() {
        // Given: an empty netflow dataset
        ProtocolProcessor processor = new ProtocolProcessor(batchConfig(), databaseOutput(), postgresWriter());

        // When: protocol aggregation runs
        processor.process(emptyFlows(), UUID.fromString("40000000-0000-0000-0000-000000000003"));

        // Then: no database rows are created
        assertThat(countRows("protocol_usage_aggregates")).isZero();
    }
}
