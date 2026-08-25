package com.nimbo.batch.writer;

import lombok.Getter;

@Getter
public class TableMapping {
    private final String tableName;
    private final String[] columnNames;
    private final String[] columnTypes;
    private final String[] conflictKeyColumns;

    private TableMapping(TableSchema schema) {
        this.tableName = schema.getTableName();
        this.columnNames = schema.getColumnNames();
        this.columnTypes = schema.getColumnTypes();
        this.conflictKeyColumns = schema.getConflictKeyColumns().toArray(String[]::new);
    }

    public static TableMapping forTrafficAggregates() {
        return new TableMapping(TableSchema.TRAFFIC_AGGREGATES);
    }

    public static TableMapping forConnectionAggregates() {
        return new TableMapping(TableSchema.CONNECTION_AGGREGATES);
    }

    public static TableMapping forProtocolUsageAggregates() {
        return new TableMapping(TableSchema.PROTOCOL_USAGE_AGGREGATES);
    }

    public static TableMapping forIpBandwidthAggregates() {
        return new TableMapping(TableSchema.IP_BANDWIDTH_AGGREGATES);
    }
}