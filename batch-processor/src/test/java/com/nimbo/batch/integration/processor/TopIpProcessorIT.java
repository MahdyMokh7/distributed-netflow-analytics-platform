package com.nimbo.batch.processor;

import com.nimbo.batch.test.support.PostgresContainerExtension;
import com.nimbo.batch.test.support.ProcessorTestFixture;
import com.nimbo.batch.test.support.SparkSessionExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class TopIpProcessorIT {

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
    void givenSourceAndDestinationFlows_whenProcessed_thenIpBandwidthPerspectivesArePersisted() {
        // Given: deterministic flows where one address appears in both perspectives
        TopIpProcessor processor = new TopIpProcessor(
                fixture.batchConfig(), fixture.databaseOutput(), fixture.postgresWriter());
        UUID batchId = UUID.fromString("30000000-0000-0000-0000-000000000001");

        // When: top-IP aggregation writes database output
        processor.process(fixture.standardFlows(), batchId);

        // Then: source and destination bandwidth are combined by IP and window
        assertThat(fixture.countRows("ip_bandwidth_aggregates")).isEqualTo(5);
        assertThat(fixture.queryLong("SELECT inbound_bytes FROM ip_bandwidth_aggregates WHERE ip = '10.0.0.1' AND outbound_bytes = 670"))
                .isEqualTo(450L);
        assertThat(fixture.queryLong("SELECT outbound_bytes FROM ip_bandwidth_aggregates WHERE ip = '10.0.0.1' AND inbound_bytes = 30"))
                .isEqualTo(20L);
    }

    @Test
    void givenTheSameBatchTwice_whenProcessed_thenIpBandwidthUpsertRemainsExactlyOnce() {
        // Given: one fixed batch identity and deterministic flow records
        TopIpProcessor processor = new TopIpProcessor(
                fixture.batchConfig(), fixture.databaseOutput(), fixture.postgresWriter());
        UUID batchId = UUID.fromString("30000000-0000-0000-0000-000000000002");

        // When: the same batch is processed twice
        processor.process(fixture.standardFlows(), batchId);
        processor.process(fixture.standardFlows(), batchId);

        // Then: the five IP-window rows are not duplicated
        assertThat(fixture.countRows("ip_bandwidth_aggregates")).isEqualTo(5);
    }

    @Test
    void givenNoFlows_whenProcessed_thenNoIpBandwidthRowsAreWritten() {
        // Given: an empty deterministic NetFlow dataset
        TopIpProcessor processor = new TopIpProcessor(
                fixture.batchConfig(), fixture.databaseOutput(), fixture.postgresWriter());

        // When: top-IP aggregation processes the empty dataset
        processor.process(
                fixture.emptyFlows(), UUID.fromString("30000000-0000-0000-0000-000000000003"));

        // Then: no IP-bandwidth rows are persisted
        assertThat(fixture.countRows("ip_bandwidth_aggregates")).isZero();
    }
}
