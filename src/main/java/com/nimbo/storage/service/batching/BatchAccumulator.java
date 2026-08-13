package com.nimbo.storage.service.batching;

import com.nimbo.ingester.model.IngestedRecord;
import com.nimbo.storage.model.TopicPartitionKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.HashMap;
import java.util.Map;

public final class BatchAccumulator {

    private static final Logger logger =
            LoggerFactory.getLogger(BatchAccumulator.class);

    private final BatchPolicy batchPolicy;
    private final List<IngestedRecord> buffer = new ArrayList<>();

    private Instant firstRecordAt;
    private long accumulatedSizeBytes;

    private final Map<TopicPartitionKey, Long> pendingOffsets = new HashMap<>();

    public BatchAccumulator(BatchPolicy batchPolicy) {
        this.batchPolicy = Objects.requireNonNull(
                batchPolicy,
                "batchPolicy must not be null"
        );
    }

    public void add(IngestedRecord record) {
        Objects.requireNonNull(
                record,
                "record must not be null"
        );

        if (buffer.isEmpty()) {
            firstRecordAt = Instant.now();
        }

        buffer.add(record);
        accumulatedSizeBytes += record.serializedSizeBytes();

        TopicPartitionKey key = new TopicPartitionKey(record.topic(), record.partition());
        pendingOffsets.merge(key, record.offset() + 1, Math::max);

//        logger.debug(
//                "Record added to batch: count={}, sizeBytes={}",
//                buffer.size(),
//                accumulatedSizeBytes
//        );
    }

    public boolean shouldFlush() {
        return batchPolicy.shouldFlush(
                buffer.size(),
                accumulatedSizeBytes,
                firstRecordAt,
                Instant.now()
        );
    }

    public RecordBatch peekBatch() {
        if (buffer.isEmpty()) {
            throw new IllegalStateException(
                    "Cannot create batch from an empty accumulator"
            );
        }

        return new RecordBatch(
                buffer,
                firstRecordAt,
                accumulatedSizeBytes
        );
    }

    public void clear() {
        if (buffer.isEmpty()) {
            return;
        }

//        logger.debug(
//                "Clearing successfully persisted batch: count={}, sizeBytes={}",
//                buffer.size(),
//                accumulatedSizeBytes
//        );

        buffer.clear();
        firstRecordAt = null;
        accumulatedSizeBytes = 0L;
        pendingOffsets.clear();
    }

    public Map<TopicPartitionKey, Long> getPendingOffsets() {
        return Map.copyOf(pendingOffsets);
    }

    public int getRecordCount() {
        return buffer.size();
    }

    public long getAccumulatedSizeBytes() {
        return accumulatedSizeBytes;
    }

    public Instant getFirstRecordAt() {
        return firstRecordAt;
    }

    public boolean isEmpty() {
        return buffer.isEmpty();
    }
}