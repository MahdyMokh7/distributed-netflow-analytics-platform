package com.nimbo.batch.writer;

import org.junit.jupiter.api.Test;

import static com.nimbo.batch.writer.ColumnTypeConstants.*;
import static org.assertj.core.api.Assertions.assertThat;

class TableSchemaTest {

    @Test
    void shouldHaveCorrectTrafficAggregatesSchema() {
        TableSchema schema = TableSchema.TRAFFIC_AGGREGATES;

        assertThat(schema.getTableName()).isEqualTo("traffic_aggregates");
        assertThat(schema.getColumnNames()).containsExactly(
                COL_SRC_IP, COL_WINDOW_START, COL_WINDOW_END,
                COL_IN_BYTES, COL_OUT_BYTES, COL_IN_PACKETS, COL_OUT_PACKETS,
                COL_RECORD_COUNT, COL_BATCH_ID, COL_INGESTED_AT
        );
        assertThat(schema.getConflictKeyColumns()).containsExactly(COL_SRC_IP, COL_WINDOW_START);
    }

    @Test
    void shouldHaveCorrectConnectionAggregatesSchema() {
        TableSchema schema = TableSchema.CONNECTION_AGGREGATES;

        assertThat(schema.getTableName()).isEqualTo("connection_aggregates");
        assertThat(schema.getColumnNames()).containsExactly(
                COL_ID, COL_SRC_IP, COL_DST_IP, COL_SRC_PORT, COL_DST_PORT,
                COL_L4_PROTOCOL, COL_WINDOW_START, COL_WINDOW_END,
                COL_IN_BYTES, COL_OUT_BYTES, COL_IN_PACKETS, COL_OUT_PACKETS,
                COL_BATCH_ID, COL_INGESTED_AT
        );
        assertThat(schema.getConflictKeyColumns()).containsExactly(COL_ID);
    }

    @Test
    void shouldHaveCorrectProtocolUsageAggregatesSchema() {
        TableSchema schema = TableSchema.PROTOCOL_USAGE_AGGREGATES;

        assertThat(schema.getTableName()).isEqualTo("protocol_usage_aggregates");
        assertThat(schema.getColumnNames()).containsExactly(
                COL_L7_PROTOCOL, COL_WINDOW_START, COL_WINDOW_END,
                COL_IN_BYTES, COL_OUT_BYTES, COL_IN_PACKETS, COL_OUT_PACKETS,
                COL_CONNECTION_COUNT, COL_BATCH_ID, COL_INGESTED_AT
        );
        assertThat(schema.getConflictKeyColumns()).containsExactly(COL_L7_PROTOCOL, COL_WINDOW_START);
    }

    @Test
    void shouldHaveCorrectIpBandwidthAggregatesSchema() {
        TableSchema schema = TableSchema.IP_BANDWIDTH_AGGREGATES;

        assertThat(schema.getTableName()).isEqualTo("ip_bandwidth_aggregates");
        assertThat(schema.getColumnNames()).containsExactly(
                COL_IP, COL_WINDOW_START, COL_WINDOW_END,
                COL_INBOUND_BYTES, COL_OUTBOUND_BYTES,
                COL_INBOUND_PACKETS, COL_OUTBOUND_PACKETS,
                COL_BATCH_ID, COL_INGESTED_AT
        );
        assertThat(schema.getConflictKeyColumns()).containsExactly(COL_IP, COL_WINDOW_START);
    }

    @Test
    void shouldReturnAllSchemas() {
        var allSchemas = TableSchema.allSchemas();

        assertThat(allSchemas).containsKeys(
                "traffic_aggregates",
                "connection_aggregates",
                "protocol_usage_aggregates",
                "ip_bandwidth_aggregates"
        );
    }

    @Test
    void shouldHaveMatchingColumnTypes() {
        TableSchema schema = TableSchema.TRAFFIC_AGGREGATES;
        String[] types = schema.getColumnTypes();

        assertThat(types).containsExactly(
                STRING, TIMESTAMP, TIMESTAMP,
                BIGINT, BIGINT, BIGINT, BIGINT,
                BIGINT, UUID, TIMESTAMP
        );
    }

    @Test
    void shouldReturnCorrectColumnTypesForConnectionSchema() {
        TableSchema schema = TableSchema.CONNECTION_AGGREGATES;
        String[] types = schema.getColumnTypes();

        assertThat(types).containsExactly(
                STRING, STRING, STRING, INTEGER, INTEGER, INTEGER,
                TIMESTAMP, TIMESTAMP, BIGINT, BIGINT, BIGINT, BIGINT,
                UUID, TIMESTAMP
        );
    }

    @Test
    void shouldReturnCorrectColumnTypesForProtocolSchema() {
        TableSchema schema = TableSchema.PROTOCOL_USAGE_AGGREGATES;
        String[] types = schema.getColumnTypes();

        assertThat(types).containsExactly(
                DOUBLE, TIMESTAMP, TIMESTAMP,
                BIGINT, BIGINT, BIGINT, BIGINT,
                BIGINT, UUID, TIMESTAMP
        );
    }

    @Test
    void shouldReturnCorrectColumnTypesForIpBandwidthSchema() {
        TableSchema schema = TableSchema.IP_BANDWIDTH_AGGREGATES;
        String[] types = schema.getColumnTypes();

        assertThat(types).containsExactly(
                STRING, TIMESTAMP, TIMESTAMP,
                BIGINT, BIGINT, BIGINT, BIGINT,
                UUID, TIMESTAMP
        );
    }

    @Test
    void shouldHaveCorrectSizeForAllSchemas() {
        assertThat(TableSchema.TRAFFIC_AGGREGATES.getColumns()).hasSize(10);
        assertThat(TableSchema.CONNECTION_AGGREGATES.getColumns()).hasSize(14);
        assertThat(TableSchema.PROTOCOL_USAGE_AGGREGATES.getColumns()).hasSize(10);
        assertThat(TableSchema.IP_BANDWIDTH_AGGREGATES.getColumns()).hasSize(9);
    }

    @Test
    void shouldReturnAllSchemasMapWithCorrectSize() {
        var allSchemas = TableSchema.allSchemas();

        assertThat(allSchemas).hasSize(4);
        assertThat(allSchemas).containsKey("traffic_aggregates");
        assertThat(allSchemas).containsKey("connection_aggregates");
        assertThat(allSchemas).containsKey("protocol_usage_aggregates");
        assertThat(allSchemas).containsKey("ip_bandwidth_aggregates");
    }
}