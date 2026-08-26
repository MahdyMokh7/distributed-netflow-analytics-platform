package com.nimbo.realtime.model;

import java.util.List;

public record StarRocksRow(
        String kafkaTopic,
        int kafkaPartition,
        long kafkaOffset,
        long kafkaTimestamp,
        List<Object> netFlowValues
) {
    public StarRocksRow {
        netFlowValues = List.copyOf(netFlowValues);
    }
}
