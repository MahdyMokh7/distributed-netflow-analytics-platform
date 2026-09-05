package com.nimbo.batch.processor;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectionProcessorTest extends BaseProcessorTest {

    @Test
    void givenDifferentPortsAndProtocolsForTheSameIpPair_whenProcessed_thenTheirMetricsCollapseIntoOneConnection() {
        // Given: deterministic flows with distinct five-tuples for one IP pair
        ConnectionProcessor processor = new ConnectionProcessor(batchConfig(), databaseOutput(), postgresWriter());
        UUID batchId = UUID.fromString("20000000-0000-0000-0000-000000000001");

        // When: connection aggregation writes database output
        processor.process(standardFlows(), batchId);

        // Then: the IP pair is merged with summed metrics and a stable SHA-256 identifier
        assertThat(countRows("connection_aggregates")).isEqualTo(3);
        assertThat(queryLong("SELECT in_bytes FROM connection_aggregates WHERE src_ip = '10.0.0.1' AND dst_ip = '10.0.0.2'")).isEqualTo(400L);
        assertThat(queryLong("SELECT out_bytes FROM connection_aggregates WHERE src_ip = '10.0.0.1' AND dst_ip = '10.0.0.2'")).isEqualTo(600L);
        assertThat(queryString("SELECT id FROM connection_aggregates WHERE src_ip = '10.0.0.1' AND dst_ip = '10.0.0.2'")).hasSize(64);
    }

    @Test
    void givenTheSameBatchTwice_whenProcessed_thenConnectionUpsertRemainsExactlyOnce() {
        // Given: one fixed batch identifier and deterministic connection records
        ConnectionProcessor processor = new ConnectionProcessor(batchConfig(), databaseOutput(), postgresWriter());
        UUID batchId = UUID.fromString("20000000-0000-0000-0000-000000000002");

        // When: the same batch is processed twice
        processor.process(standardFlows(), batchId);
        processor.process(standardFlows(), batchId);

        // Then: the three IP-pair connections are not duplicated
        assertThat(countRows("connection_aggregates")).isEqualTo(3);
    }

    @Test
    void givenNoFlows_whenProcessed_thenNoConnectionRowsAreWritten() {
        // Given: an empty netflow dataset
        ConnectionProcessor processor = new ConnectionProcessor(batchConfig(), databaseOutput(), postgresWriter());

        // When: connection aggregation runs
        processor.process(emptyFlows(), UUID.fromString("20000000-0000-0000-0000-000000000003"));

        // Then: no database rows are created
        assertThat(countRows("connection_aggregates")).isZero();
    }
}
