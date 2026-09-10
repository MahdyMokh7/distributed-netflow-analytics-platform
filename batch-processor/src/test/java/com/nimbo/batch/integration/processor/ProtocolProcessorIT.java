package com.nimbo.batch.processor;

import com.nimbo.batch.test.support.PostgresContainerExtension;
import com.nimbo.batch.test.support.ProcessorTestFixture;
import com.nimbo.batch.test.support.SparkSessionExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ProtocolProcessorIT {

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
    void givenFlowsAcrossProtocolsAndWindows_whenProcessed_thenProtocolUsageIsPersisted() {
        // Given: deterministic TCP and UDP flows across two windows
        ProtocolProcessor processor = new ProtocolProcessor(
                fixture.batchConfig(), fixture.databaseOutput(), fixture.postgresWriter());
        UUID batchId = UUID.fromString("40000000-0000-0000-0000-000000000001");

        // When: protocol aggregation writes database output
        processor.process(fixture.standardFlows(), batchId);

        // Then: protocol-window metrics exactly match the input flows
        assertThat(fixture.countRows("protocol_usage_aggregates")).isEqualTo(3);
        assertThat(fixture.queryLong("SELECT connection_count FROM protocol_usage_aggregates WHERE l7_protocol = 6.0 AND in_bytes = 400"))
                .isEqualTo(2L);
        assertThat(fixture.queryLong("SELECT out_bytes FROM protocol_usage_aggregates WHERE l7_protocol = 6.0 AND in_bytes = 400"))
                .isEqualTo(600L);
    }

    @Test
    void givenTheSameBatchTwice_whenProcessed_thenProtocolUpsertRemainsExactlyOnce() {
        // Given: one fixed batch identity and deterministic flow records
        ProtocolProcessor processor = new ProtocolProcessor(
                fixture.batchConfig(), fixture.databaseOutput(), fixture.postgresWriter());
        UUID batchId = UUID.fromString("40000000-0000-0000-0000-000000000002");

        // When: the same batch is processed twice
        processor.process(fixture.standardFlows(), batchId);
        processor.process(fixture.standardFlows(), batchId);

        // Then: the three protocol-window rows are not duplicated
        assertThat(fixture.countRows("protocol_usage_aggregates")).isEqualTo(3);
    }

    @Test
    void givenNoFlows_whenProcessed_thenNoProtocolRowsAreWritten() {
        // Given: an empty deterministic NetFlow dataset
        ProtocolProcessor processor = new ProtocolProcessor(
                fixture.batchConfig(), fixture.databaseOutput(), fixture.postgresWriter());

        // When: protocol aggregation processes the empty dataset
        processor.process(
                fixture.emptyFlows(), UUID.fromString("40000000-0000-0000-0000-000000000003"));

        // Then: no protocol rows are persisted
        assertThat(fixture.countRows("protocol_usage_aggregates")).isZero();
    }
}
