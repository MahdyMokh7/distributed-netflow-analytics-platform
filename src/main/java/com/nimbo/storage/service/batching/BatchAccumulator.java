package com.nimbo.storage.service.batching;

import com.nimbo.ingester.model.IngestedRecord;
import com.nimbo.storage.model.TopicPartitionKey;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public final class BatchAccumulator {

    private final BatchPolicy batchPolicy;

    private Instant firstRecordAt;
    private long accumulatedSizeBytes;
    private long recordCount;

    // Nested map: topic -> partition -> nextOffset
    private final Map<String, Map<Integer, Long>> pendingOffsets = new HashMap<>();

    public BatchAccumulator(BatchPolicy batchPolicy) {
        this.batchPolicy = Objects.requireNonNull(
                batchPolicy,
                "batchPolicy must not be null"
        );
    }

    public void add(IngestedRecord record) {
        Objects.requireNonNull(record, "record must not be null");

        if (firstRecordAt == null) {
            firstRecordAt = Instant.now();
        }

        recordCount++;
        accumulatedSizeBytes += record.serializedSizeBytes();

        pendingOffsets
                .computeIfAbsent(record.topic(), t -> new HashMap<>())
                .merge(record.partition(), record.offset() + 1, Math::max);
    }

    public boolean shouldFlush() {
        return batchPolicy.shouldFlush(
                recordCount,
                accumulatedSizeBytes,
                firstRecordAt,
                Instant.now()
        );
    }

    public void clear() {
        if (recordCount == 0) {
            return;
        }

        recordCount = 0;
        accumulatedSizeBytes = 0L;
        firstRecordAt = null;
        pendingOffsets.clear();
    }

    public Map<TopicPartitionKey, Long> getPendingOffsets() {
        Map<TopicPartitionKey, Long> result = new HashMap<>();
        pendingOffsets.forEach((topic, partitions) ->
                partitions.forEach((partition, offset) ->
                        result.put(
                                new TopicPartitionKey(topic, partition),
                                offset
                        )
                )
        );
        return Map.copyOf(result);
    }

    public long getRecordCount() {
        return recordCount;
    }

    public long getAccumulatedSizeBytes() {
        return accumulatedSizeBytes;
    }

    public Instant getFirstRecordAt() {
        return firstRecordAt;
    }

    public boolean isEmpty() {
        return recordCount == 0;
    }
}