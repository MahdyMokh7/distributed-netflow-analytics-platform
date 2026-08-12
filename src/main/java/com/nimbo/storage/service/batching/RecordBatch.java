package com.nimbo.storage.service.batching;

import com.nimbo.ingester.model.IngestedRecord;
import com.nimbo.storage.model.TopicPartitionKey;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class RecordBatch {

    private final List<IngestedRecord> records;
    private final Instant createdAt;
    private final long totalSizeBytes;

    public RecordBatch(
            List<IngestedRecord> records,
            Instant createdAt,
            long totalSizeBytes
    ) {
        Objects.requireNonNull(records, "records must not be null");
        Objects.requireNonNull(createdAt, "createdAt must not be null");

        if (records.isEmpty()) {
            throw new IllegalArgumentException(
                    "records must not be empty"
            );
        }

        if (totalSizeBytes < 0) {
            throw new IllegalArgumentException(
                    "totalSizeBytes must not be negative"
            );
        }

        this.records = List.copyOf(records);
        this.createdAt = createdAt;
        this.totalSizeBytes = totalSizeBytes;
    }

    public List<IngestedRecord> getRecords() {
        return records;
    }

    public int size() {
        return records.size();
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public long getTotalSizeBytes() {
        return totalSizeBytes;
    }

    public Map<TopicPartitionKey, Long> getOffsetsToCommit() {
        Map<TopicPartitionKey, Long> commitOffsets = new HashMap<>();
        for (IngestedRecord rec : records) {
            TopicPartitionKey key = new TopicPartitionKey(rec.topic(), rec.partition());
            long nextOffset = rec.offset() + 1;
            commitOffsets.merge(key, nextOffset, Math::max);
        }
        return Map.copyOf(commitOffsets);
    }
}