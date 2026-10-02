package com.nimbo.starrockswriter.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public record StarRocksRow(
        String kafkaTopic,
        int kafkaPartition,
        long kafkaOffset,
        long processTime,
        Map<String, Object> netFlowValues
) {
    public StarRocksRow {
        netFlowValues = new LinkedHashMap<>(netFlowValues);
    }

    public List<Object> values(List<String> netFlowColumns) {
        List<Object> values = new ArrayList<>(4 + netFlowColumns.size());
        values.add(kafkaTopic);
        values.add(kafkaPartition);
        values.add(kafkaOffset);
        values.add(processTime);

        for (String column : netFlowColumns) {
            values.add(netFlowValues.get(column));
        }
        return values;
    }
}
