package com.nimbo.batch.writer;

import lombok.Getter;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

import static com.nimbo.batch.writer.ColumnTypeConstants.*;

@Getter
public class TableSchema {
    private final String tableName;
    private final List<Column> columns;
    private final List<String> conflictKeyColumns;

    private TableSchema(String tableName, List<Column> columns, List<String> conflictKeyColumns) {
        this.tableName = tableName;
        this.columns = columns;
        this.conflictKeyColumns = conflictKeyColumns;
    }

    public String[] getColumnNames() {
        return columns.stream()
                .map(Column::getName)
                .toArray(String[]::new);
    }

    public String[] getColumnTypes() {
        return columns.stream()
                .map(Column::getType)
                .toArray(String[]::new);
    }

    public static class Column {
        private final String name;
        private final String type;

        private Column(String name, String type) {
            this.name = name;
            this.type = type;
        }

        public String getName() { return name; }
        public String getType() { return type; }
    }

    public static final TableSchema TRAFFIC_AGGREGATES = schema("traffic_aggregates")
            .column(COL_SRC_IP, STRING)
            .column(COL_WINDOW_START, TIMESTAMP)
            .column(COL_WINDOW_END, TIMESTAMP)
            .column(COL_IN_BYTES, BIGINT)
            .column(COL_OUT_BYTES, BIGINT)
            .column(COL_IN_PACKETS, BIGINT)
            .column(COL_OUT_PACKETS, BIGINT)
            .column(COL_RECORD_COUNT, BIGINT)
            .column(COL_BATCH_ID, UUID)
            .column(COL_INGESTED_AT, TIMESTAMP)
            .conflictKeys(COL_SRC_IP, COL_WINDOW_START)
            .build();

    public static final TableSchema CONNECTION_AGGREGATES = schema("connection_aggregates")
            .column(COL_ID, STRING)
            .column(COL_SRC_IP, STRING)
            .column(COL_DST_IP, STRING)
            .column(COL_SRC_PORT, INTEGER)
            .column(COL_DST_PORT, INTEGER)
            .column(COL_L4_PROTOCOL, INTEGER)
            .column(COL_WINDOW_START, TIMESTAMP)
            .column(COL_WINDOW_END, TIMESTAMP)
            .column(COL_IN_BYTES, BIGINT)
            .column(COL_OUT_BYTES, BIGINT)
            .column(COL_IN_PACKETS, BIGINT)
            .column(COL_OUT_PACKETS, BIGINT)
            .column(COL_BATCH_ID, UUID)
            .column(COL_INGESTED_AT, TIMESTAMP)
            .conflictKeys(COL_ID)
            .build();

    public static final TableSchema PROTOCOL_USAGE_AGGREGATES = schema("protocol_usage_aggregates")
            .column(COL_L7_PROTOCOL, DOUBLE)
            .column(COL_WINDOW_START, TIMESTAMP)
            .column(COL_WINDOW_END, TIMESTAMP)
            .column(COL_IN_BYTES, BIGINT)
            .column(COL_OUT_BYTES, BIGINT)
            .column(COL_IN_PACKETS, BIGINT)
            .column(COL_OUT_PACKETS, BIGINT)
            .column(COL_CONNECTION_COUNT, BIGINT)
            .column(COL_BATCH_ID, UUID)
            .column(COL_INGESTED_AT, TIMESTAMP)
            .conflictKeys(COL_L7_PROTOCOL, COL_WINDOW_START)
            .build();

    public static final TableSchema IP_BANDWIDTH_AGGREGATES = schema("ip_bandwidth_aggregates")
            .column(COL_IP, STRING)
            .column(COL_WINDOW_START, TIMESTAMP)
            .column(COL_WINDOW_END, TIMESTAMP)
            .column(COL_INBOUND_BYTES, BIGINT)
            .column(COL_OUTBOUND_BYTES, BIGINT)
            .column(COL_INBOUND_PACKETS, BIGINT)
            .column(COL_OUTBOUND_PACKETS, BIGINT)
            .column(COL_BATCH_ID, UUID)
            .column(COL_INGESTED_AT, TIMESTAMP)
            .conflictKeys(COL_IP, COL_WINDOW_START)
            .build();

    private static SchemaBuilder schema(String tableName) {
        return new SchemaBuilder(tableName);
    }

    private static class SchemaBuilder {
        private final String tableName;
        private final List<Column> columns;
        private List<String> conflictKeys;

        private SchemaBuilder(String tableName) {
            this.tableName = tableName;
            this.columns = new ArrayList<>();
        }

        private SchemaBuilder column(String name, String type) {
            columns.add(new Column(name, type));
            return this;
        }

        private SchemaBuilder conflictKeys(String... keys) {
            this.conflictKeys = Arrays.asList(keys);
            return this;
        }

        private TableSchema build() {
            return new TableSchema(tableName, columns, conflictKeys);
        }
    }

    public static Map<String, TableSchema> allSchemas() {
        return Map.of(
                TRAFFIC_AGGREGATES.getTableName(), TRAFFIC_AGGREGATES,
                CONNECTION_AGGREGATES.getTableName(), CONNECTION_AGGREGATES,
                PROTOCOL_USAGE_AGGREGATES.getTableName(), PROTOCOL_USAGE_AGGREGATES,
                IP_BANDWIDTH_AGGREGATES.getTableName(), IP_BANDWIDTH_AGGREGATES
        );
    }
}