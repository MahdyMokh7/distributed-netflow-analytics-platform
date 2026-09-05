package com.nimbo.batch.processor;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class TopIpProcessorTest extends BaseProcessorTest {

    @Test
    void givenSourceAndDestinationFlows_whenProcessed_thenIpBandwidthPerspectivesArePersisted() {
        // Given: deterministic flows where one address appears in both perspectives
        TopIpProcessor processor = new TopIpProcessor(batchConfig(), databaseOutput(), postgresWriter());
        UUID batchId = UUID.fromString("30000000-0000-0000-0000-000000000001");

        // When: top-IP aggregation writes database output
        processor.process(standardFlows(), batchId);

        // Then: source and destination bandwidth are combined by IP and window
        assertThat(countRows("ip_bandwidth_aggregates")).isEqualTo(5);
        assertThat(queryLong("SELECT inbound_bytes FROM ip_bandwidth_aggregates WHERE ip = '10.0.0.1' AND outbound_bytes = 670")).isEqualTo(450L);
        assertThat(queryLong("SELECT outbound_bytes FROM ip_bandwidth_aggregates WHERE ip = '10.0.0.1' AND inbound_bytes = 30")).isEqualTo(20L);
    }

    @Test
    void givenTheSameBatchTwice_whenProcessed_thenIpBandwidthUpsertRemainsExactlyOnce() {
        // Given: one fixed batch identifier and deterministic flow records
        TopIpProcessor processor = new TopIpProcessor(batchConfig(), databaseOutput(), postgresWriter());
        UUID batchId = UUID.fromString("30000000-0000-0000-0000-000000000002");

        // When: the same batch is processed twice
        processor.process(standardFlows(), batchId);
        processor.process(standardFlows(), batchId);

        // Then: the five IP-window rows are not duplicated
        assertThat(countRows("ip_bandwidth_aggregates")).isEqualTo(5);
    }

    @Test
    void givenNoFlows_whenProcessed_thenNoIpBandwidthRowsAreWritten() {
        // Given: an empty netflow dataset
        TopIpProcessor processor = new TopIpProcessor(batchConfig(), databaseOutput(), postgresWriter());

        // When: top-IP aggregation runs
        processor.process(emptyFlows(), UUID.fromString("30000000-0000-0000-0000-000000000003"));

        // Then: no database rows are created
        assertThat(countRows("ip_bandwidth_aggregates")).isZero();
    }
}
