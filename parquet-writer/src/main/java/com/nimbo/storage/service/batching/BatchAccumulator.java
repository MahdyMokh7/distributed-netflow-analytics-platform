package com.nimbo.storage.service.batching;

import com.nimbo.ingester.model.IngestedRecord;
import com.nimbo.storage.model.TopicPartitionKey;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class BatchAccumulator {

  private final BatchPolicy batchPolicy;

  @Getter private Instant firstRecordAt;
  @Getter private long accumulatedSizeBytes;
  @Getter private long recordCount;

  // Nested map: topic -> partition -> nextOffset
  private final Map<String, Map<Integer, Long>> pendingOffsets = new HashMap<>();

  public BatchAccumulator(BatchPolicy batchPolicy) {
    this.batchPolicy = Objects.requireNonNull(batchPolicy, "batchPolicy must not be null");
  }

  public void add(IngestedRecord ingestedRecord) {
    Objects.requireNonNull(ingestedRecord, "record must not be null");

    if (firstRecordAt == null) {
      firstRecordAt = Instant.now();
    }

    recordCount++;
    accumulatedSizeBytes += ingestedRecord.serializedSizeBytes();

    pendingOffsets
        .computeIfAbsent(ingestedRecord.topic(), t -> new HashMap<>())
        .merge(ingestedRecord.partition(), ingestedRecord.offset() + 1, Math::max);
  }

  public boolean shouldFlush() {
    boolean flush =
        batchPolicy.shouldFlush(recordCount, accumulatedSizeBytes, firstRecordAt, Instant.now());

    if (flush) {
      log.info(
          "Batch flush triggered: recordCount={}, accumulatedSizeBytes={}, firstRecordAt={}",
          recordCount,
          accumulatedSizeBytes,
          firstRecordAt);
    }

    return flush;
  }

  public BatchPolicy.FlushReason flushReason() {
    return batchPolicy.flushReason(recordCount, accumulatedSizeBytes, firstRecordAt, Instant.now());
  }

  public void clear() {
    recordCount = 0;
    accumulatedSizeBytes = 0L;
    firstRecordAt = null;
    pendingOffsets.clear();
  }

  public Optional<Map<TopicPartitionKey, Long>> recordSkippedOffset(
      String topic, int partition, long nextOffset) {

    if (firstRecordAt == null) {
      firstRecordAt = Instant.now();
    }

    pendingOffsets
        .computeIfAbsent(topic, t -> new HashMap<>())
        .merge(partition, nextOffset, Math::max);

    return Optional.empty();
  }

  public Map<TopicPartitionKey, Long> getPendingOffsets() {
    Map<TopicPartitionKey, Long> result = new HashMap<>();
    pendingOffsets.forEach(
        (topic, partitions) ->
            partitions.forEach(
                (partition, offset) ->
                    result.put(new TopicPartitionKey(topic, partition), offset)));
    return Map.copyOf(result);
  }

  public boolean isEmpty() {
    return recordCount == 0;
  }
}
