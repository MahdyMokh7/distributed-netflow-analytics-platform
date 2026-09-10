package com.nimbo.batch.processor;

import com.nimbo.batch.support.PostgresContainerExtension;
import com.nimbo.batch.support.ProcessorTestFixture;
import com.nimbo.batch.support.SparkSessionExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectionProcessorIT {

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
    void givenDifferentPortsAndProtocolsForTheSameIpPair_whenProcessed_thenTheirMetricsCollapseIntoOneConnection() {
        // Given: deterministic flows with distinct five-tuples for one IP pair
        ConnectionProcessor processor = new ConnectionProcessor(
                fixture.batchConfig(), fixture.databaseOutput(), fixture.postgresWriter());
        UUID batchId = UUID.fromString("20000000-0000-0000-0000-000000000001");

        // When: connection aggregation writes database output
        processor.process(fixture.standardFlows(), batchId);

        // Then: the IP pair is merged with exact metrics and a stable identifier
        assertThat(fixture.countRows("connection_aggregates")).isEqualTo(3);
        assertThat(fixture.queryLong("SELECT COUNT(*) FROM connection_aggregates WHERE src_ip = '10.0.0.1' AND dst_ip = '10.0.0.2'"))
                .isEqualTo(1L);
        assertThat(fixture.queryLong("SELECT in_bytes FROM connection_aggregates WHERE src_ip = '10.0.0.1' AND dst_ip = '10.0.0.2'"))
                .isEqualTo(400L);
        assertThat(fixture.queryLong("SELECT out_bytes FROM connection_aggregates WHERE src_ip = '10.0.0.1' AND dst_ip = '10.0.0.2'"))
                .isEqualTo(600L);
        assertThat(fixture.queryString("SELECT id FROM connection_aggregates WHERE src_ip = '10.0.0.1' AND dst_ip = '10.0.0.2'"))
                .hasSize(64);
    }

    @Test
    void givenTheSameBatchTwice_whenProcessed_thenConnectionUpsertRemainsExactlyOnce() {
        // Given: one fixed batch identity and deterministic connection records
        ConnectionProcessor processor = new ConnectionProcessor(
                fixture.batchConfig(), fixture.databaseOutput(), fixture.postgresWriter());
        UUID batchId = UUID.fromString("20000000-0000-0000-0000-000000000002");

        // When: the same batch is processed twice
        processor.process(fixture.standardFlows(), batchId);
        processor.process(fixture.standardFlows(), batchId);

        // Then: the three IP-pair connections are not duplicated
        assertThat(fixture.countRows("connection_aggregates")).isEqualTo(3);
    }

    @Test
    void givenNoFlows_whenProcessed_thenNoConnectionRowsAreWritten() {
        // Given: an empty deterministic NetFlow dataset
        ConnectionProcessor processor = new ConnectionProcessor(
                fixture.batchConfig(), fixture.databaseOutput(), fixture.postgresWriter());

        // When: connection aggregation processes the empty dataset
        processor.process(
                fixture.emptyFlows(), UUID.fromString("20000000-0000-0000-0000-000000000003"));

        // Then: no connection rows are persisted
        assertThat(fixture.countRows("connection_aggregates")).isZero();
    }
}
