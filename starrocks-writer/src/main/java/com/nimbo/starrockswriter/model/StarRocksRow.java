package com.nimbo.starrockswriter.model;

import java.util.ArrayList;
import java.util.List;

public record StarRocksRow(
        String kafkaTopic,
        int kafkaPartition,
        long kafkaOffset,
        long kafkaTimestamp,
        List<Object> netFlowValues
) {
    public StarRocksRow {
        netFlowValues = new ArrayList<>(netFlowValues);
    }

    public List<Object> values() {

        List<Object> values = new ArrayList<>();

        values.add(kafkaTopic);
        values.add(kafkaPartition);
        values.add(kafkaOffset);
        values.add(kafkaTimestamp);

        values.addAll(netFlowValues);

        return values;
    }
}
