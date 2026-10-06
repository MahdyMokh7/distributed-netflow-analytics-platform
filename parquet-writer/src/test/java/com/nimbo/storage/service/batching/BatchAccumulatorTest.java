package com.nimbo.storage.service.batching;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nimbo.ingester.model.IngestedRecord;
import com.nimbo.storage.model.TopicPartitionKey;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

class BatchAccumulatorTest {

  private static final Schema TEST_SCHEMA = Schema.createRecord("TestRecord", null, null, false);
  private static final long KAFKA_TIMESTAMP = 1_700_000_000_000L;

  static {
    TEST_SCHEMA.setFields(List.of());
  }

  private static final BatchPolicy DEFAULT_POLICY =
      new BatchPolicy(100, 1_000, Duration.ofMinutes(1));

  private IngestedRecord createRecord(String topic, int partition, long offset, int sizeBytes) {
    GenericRecord avroRecord = new GenericData.Record(TEST_SCHEMA);

    return new IngestedRecord(avroRecord, topic, partition, offset, KAFKA_TIMESTAMP, sizeBytes);
  }

  private IngestedRecord createRecord(int sizeBytes) {
    return createRecord("multi-version-netflow", 0, 100L, sizeBytes);
  }

  @Test
  void shouldStartEmpty() {

    BatchAccumulator accumulator = new BatchAccumulator(DEFAULT_POLICY);

    assertTrue(accumulator.isEmpty());
    assertEquals(0, accumulator.getRecordCount());
    assertEquals(0, accumulator.getAccumulatedSizeBytes());
    assertNull(accumulator.getFirstRecordAt());
  }

  @Test
  void shouldAddRecord() {

    BatchAccumulator accumulator = new BatchAccumulator(DEFAULT_POLICY);

    accumulator.add(createRecord(100));

    assertFalse(accumulator.isEmpty());
    assertEquals(1, accumulator.getRecordCount());
    assertEquals(100, accumulator.getAccumulatedSizeBytes());
    assertNotNull(accumulator.getFirstRecordAt());
  }

  @Test
  void shouldAccumulateRecordSizes() {

    BatchAccumulator accumulator = new BatchAccumulator(DEFAULT_POLICY);

    accumulator.add(createRecord(100));
    accumulator.add(createRecord(250));
    accumulator.add(createRecord(50));

    assertEquals(3, accumulator.getRecordCount());
    assertEquals(400, accumulator.getAccumulatedSizeBytes());
  }

  @Test
  void shouldKeepFirstRecordTimestamp() {

    BatchAccumulator accumulator = new BatchAccumulator(DEFAULT_POLICY);
    accumulator.add(createRecord(100));
    Instant firstTimestamp = accumulator.getFirstRecordAt();
    accumulator.add(createRecord(100));

    assertEquals(firstTimestamp, accumulator.getFirstRecordAt());
  }

  @Test
  void shouldNotFlushEmptyAccumulator() {
    BatchPolicy policy = new BatchPolicy(1, 1, Duration.ofNanos(1));
    BatchAccumulator accumulator = new BatchAccumulator(policy);
    assertFalse(accumulator.shouldFlush());
  }

  @Test
  void shouldFlushWhenRecordCountReached() {
    BatchPolicy policy = new BatchPolicy(2, 1000, Duration.ofMinutes(1));
    BatchAccumulator accumulator = new BatchAccumulator(policy);
    accumulator.add(createRecord(100));

    assertFalse(accumulator.shouldFlush());

    accumulator.add(createRecord(100));

    assertTrue(accumulator.shouldFlush());
  }

  @Test
  void shouldFlushWhenSizeReached() {
    BatchPolicy policy = new BatchPolicy(100, 500, Duration.ofMinutes(1));

    BatchAccumulator accumulator = new BatchAccumulator(policy);

    accumulator.add(createRecord(300));
    assertFalse(accumulator.shouldFlush());

    accumulator.add(createRecord(200));

    assertTrue(accumulator.shouldFlush());
  }

  @Test
  void shouldClearAccumulator() {

    BatchAccumulator accumulator = new BatchAccumulator(DEFAULT_POLICY);

    accumulator.add(createRecord("multi-version-netflow", 0, 10L, 100));
    accumulator.add(createRecord("multi-version-netflow", 1, 20L, 200));

    accumulator.clear();

    assertTrue(accumulator.isEmpty());
    assertEquals(0, accumulator.getRecordCount());
    assertEquals(0, accumulator.getAccumulatedSizeBytes());
    assertNull(accumulator.getFirstRecordAt());
    assertTrue(accumulator.getPendingOffsets().isEmpty());
  }

  @Test
  void clearingEmptyAccumulatorShouldBeSafe() {

    BatchAccumulator accumulator = new BatchAccumulator(DEFAULT_POLICY);

    assertDoesNotThrow(accumulator::clear);
    assertTrue(accumulator.isEmpty());
  }

  @Test
  void shouldRejectNullPolicy() {
    assertThrows(NullPointerException.class, () -> new BatchAccumulator(null));
  }

  @Test
  void shouldRejectNullRecord() {

    BatchAccumulator accumulator = new BatchAccumulator(DEFAULT_POLICY);

    assertThrows(NullPointerException.class, () -> accumulator.add(null));
  }

  @Test
  void shouldTrackNextOffsetForPartition() {

    BatchAccumulator accumulator = new BatchAccumulator(DEFAULT_POLICY);
    accumulator.add(createRecord("multi-version-netflow", 0, 10L, 100));

    Map<TopicPartitionKey, Long> offsets = accumulator.getPendingOffsets();

    assertEquals(1, offsets.size());
    assertEquals(11L, offsets.get(new TopicPartitionKey("multi-version-netflow", 0)));
  }

  @Test
  void shouldTrackHighestOffsetForSamePartition() {

    BatchAccumulator accumulator = new BatchAccumulator(DEFAULT_POLICY);

    accumulator.add(createRecord("multi-version-netflow", 0, 15L, 100));
    accumulator.add(createRecord("multi-version-netflow", 0, 10L, 100));
    accumulator.add(createRecord("multi-version-netflow", 0, 12L, 100));

    Map<TopicPartitionKey, Long> offsets = accumulator.getPendingOffsets();

    assertEquals(1, offsets.size());
    assertEquals(16L, offsets.get(new TopicPartitionKey("multi-version-netflow", 0)));
  }

  @Test
  void shouldTrackOffsetsForMultiplePartitions() {

    BatchAccumulator accumulator = new BatchAccumulator(DEFAULT_POLICY);

    accumulator.add(createRecord("multi-version-netflow", 0, 10L, 100));
    accumulator.add(createRecord("multi-version-netflow", 1, 20L, 100));
    accumulator.add(createRecord("multi-version-netflow", 2, 30L, 100));

    Map<TopicPartitionKey, Long> offsets = accumulator.getPendingOffsets();

    assertEquals(3, offsets.size());
    assertEquals(11L, offsets.get(new TopicPartitionKey("multi-version-netflow", 0)));
    assertEquals(21L, offsets.get(new TopicPartitionKey("multi-version-netflow", 1)));
    assertEquals(31L, offsets.get(new TopicPartitionKey("multi-version-netflow", 2)));
  }

  @Test
  void shouldTrackOffsetsForDifferentTopics() {

    BatchAccumulator accumulator = new BatchAccumulator(DEFAULT_POLICY);

    accumulator.add(createRecord("topic-a", 0, 10L, 100));
    accumulator.add(createRecord("topic-b", 0, 20L, 100));

    Map<TopicPartitionKey, Long> offsets = accumulator.getPendingOffsets();

    assertEquals(2, offsets.size());
    assertEquals(11L, offsets.get(new TopicPartitionKey("topic-a", 0)));
    assertEquals(21L, offsets.get(new TopicPartitionKey("topic-b", 0)));
  }

  @Test
  void shouldRecordSkippedOffsetWithoutAddingRecord() {

    BatchAccumulator accumulator = new BatchAccumulator(DEFAULT_POLICY);

    accumulator.recordSkippedOffset("multi-version-netflow", 0, 11L);

    assertEquals(
        Map.of(new TopicPartitionKey("multi-version-netflow", 0), 11L),
        accumulator.getPendingOffsets());
    assertEquals(0, accumulator.getRecordCount());
    assertEquals(0, accumulator.getAccumulatedSizeBytes());
  }

  @Test
  void shouldKeepHighestOffsetBetweenSkippedAndValidRecords() {
    BatchAccumulator accumulator = new BatchAccumulator(DEFAULT_POLICY);

    accumulator.recordSkippedOffset("multi-version-netflow", 0, 20L);
    accumulator.add(createRecord("multi-version-netflow", 0, 10L, 100));
    assertEquals(
        20L,
        accumulator.getPendingOffsets().get(new TopicPartitionKey("multi-version-netflow", 0)));
  }

  @Test
  void shouldReturnUnmodifiablePendingOffsetsMap() {

    BatchAccumulator accumulator = new BatchAccumulator(DEFAULT_POLICY);

    accumulator.add(createRecord("multi-version-netflow", 0, 10L, 100));
    Map<TopicPartitionKey, Long> offsets = accumulator.getPendingOffsets();

    assertThrows(UnsupportedOperationException.class, () -> offsets.clear());
  }
}
