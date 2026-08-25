package com.nimbo.batch.writer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TableMappingTest {

    @Test
    void shouldCreateTrafficAggregatesMapping() {
        TableMapping mapping = TableMapping.forTrafficAggregates();

        assertThat(mapping.getTableName()).isEqualTo("traffic_aggregates");
        assertThat(mapping.getColumnNames()).hasSize(10);
        assertThat(mapping.getConflictKeyColumns()).containsExactly("src_ip", "window_start");
    }

    @Test
    void shouldCreateConnectionAggregatesMapping() {
        TableMapping mapping = TableMapping.forConnectionAggregates();

        assertThat(mapping.getTableName()).isEqualTo("connection_aggregates");
        assertThat(mapping.getColumnNames()).hasSize(14);
        assertThat(mapping.getConflictKeyColumns()).containsExactly("id");
    }

    @Test
    void shouldCreateProtocolUsageAggregatesMapping() {
        TableMapping mapping = TableMapping.forProtocolUsageAggregates();

        assertThat(mapping.getTableName()).isEqualTo("protocol_usage_aggregates");
        assertThat(mapping.getColumnNames()).hasSize(10);
        assertThat(mapping.getConflictKeyColumns()).containsExactly("l7_protocol", "window_start");
    }

    @Test
    void shouldCreateIpBandwidthAggregatesMapping() {
        TableMapping mapping = TableMapping.forIpBandwidthAggregates();

        assertThat(mapping.getTableName()).isEqualTo("ip_bandwidth_aggregates");
        assertThat(mapping.getColumnNames()).hasSize(9);
        assertThat(mapping.getConflictKeyColumns()).containsExactly("ip", "window_start");
    }

    @Test
    void shouldHaveCorrectColumnTypesForTrafficMapping() {
        TableMapping mapping = TableMapping.forTrafficAggregates();
        String[] types = mapping.getColumnTypes();

        assertThat(types).containsExactly(
                "string", "timestamp", "timestamp",
                "bigint", "bigint", "bigint", "bigint",
                "bigint", "uuid", "timestamp"
        );
    }

    @Test
    void shouldHaveCorrectColumnTypesForConnectionMapping() {
        TableMapping mapping = TableMapping.forConnectionAggregates();
        String[] types = mapping.getColumnTypes();

        assertThat(types).containsExactly(
                "string", "string", "string", "integer", "integer", "integer",
                "timestamp", "timestamp", "bigint", "bigint", "bigint", "bigint",
                "uuid", "timestamp"
        );
    }

    @Test
    void shouldHaveCorrectColumnTypesForProtocolMapping() {
        TableMapping mapping = TableMapping.forProtocolUsageAggregates();
        String[] types = mapping.getColumnTypes();

        assertThat(types).containsExactly(
                "double", "timestamp", "timestamp",
                "bigint", "bigint", "bigint", "bigint",
                "bigint", "uuid", "timestamp"
        );
    }

    @Test
    void shouldHaveCorrectColumnTypesForIpBandwidthMapping() {
        TableMapping mapping = TableMapping.forIpBandwidthAggregates();
        String[] types = mapping.getColumnTypes();

        assertThat(types).containsExactly(
                "string", "timestamp", "timestamp",
                "bigint", "bigint", "bigint", "bigint",
                "uuid", "timestamp"
        );
    }

    @Test
    void shouldReturnSameInstanceForSameTable() {
        TableMapping first = TableMapping.forTrafficAggregates();
        TableMapping second = TableMapping.forTrafficAggregates();

        assertThat(first.getTableName()).isEqualTo(second.getTableName());
        assertThat(first.getColumnNames()).isEqualTo(second.getColumnNames());
    }
}