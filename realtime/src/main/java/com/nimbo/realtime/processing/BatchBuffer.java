package com.nimbo.realtime.processing;

import com.nimbo.realtime.config.StarRocksProperties;
import com.nimbo.realtime.model.StarRocksRow;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RequiredArgsConstructor
public class BatchBuffer {

    private final StarRocksProperties properties;
    private final List<StarRocksRow> rows = new ArrayList<>();
    private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    private long lastFlushTime = System.currentTimeMillis();

    public void add(StarRocksRow row, TopicPartition partition, OffsetAndMetadata offset) {
        rows.add(row);
        offsets.put(partition, offset);
    }

    public void addOffset(TopicPartition partition, OffsetAndMetadata offset) {
        offsets.put(partition, offset);
    }

    public boolean shouldFlush() {
        return flushReason() != null;
    }

    public FlushReason flushReason() {
        if (reachedSize()) return FlushReason.SIZE;
        if (reachedTime()) return FlushReason.TIME;
        return null;
    }

    private boolean reachedSize() {
        return rows.size() >= properties.getMaxBatchSizeRows();
    }

    private boolean reachedTime() {
        if (rows.isEmpty()) return false;
        return System.currentTimeMillis() - lastFlushTime >= properties.getMaxBatchTimeMs();
    }

    public boolean isEmpty() {
        return rows.isEmpty();
    }

    public int rowCount() { return rows.size(); }

    public long ageSeconds() {
        return rows.isEmpty() ? 0 : (System.currentTimeMillis() - lastFlushTime) / 1000;
    }

    public enum FlushReason { SIZE, TIME, SHUTDOWN }

    public BatchSnapshot snapshot() {
        return new BatchSnapshot(List.copyOf(rows), Map.copyOf(offsets));
    }

    public void clear() {
        rows.clear();
        offsets.clear();
        lastFlushTime = System.currentTimeMillis();
    }

    public record BatchSnapshot(
            List<StarRocksRow> rows,
            Map<TopicPartition, OffsetAndMetadata> offsets
    ) {}
}
