package com.nimbo.storage.service.batching;

import com.nimbo.ingester.model.IngestedRecord;
import org.apache.avro.generic.GenericRecord;
import com.nimbo.storage.model.TopicPartitionKey;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class BatchAccumulatorTest {

    private IngestedRecord createRecord(String topic, int partition, long offset, int sizeBytes) {
        GenericRecord avroRecord = mock(GenericRecord.class);
        return new IngestedRecord(
                avroRecord,
                topic,
                partition,
                offset,
                sizeBytes
        );
    }

    private IngestedRecord createRecord(int sizeBytes) {
        return createRecord("netflow", 0, 100L, sizeBytes);
    }

    @Test
    void shouldStartEmpty() {
        BatchPolicy policy = new BatchPolicy(
                100,
                1000,
                Duration.ofMinutes(1)
        );

        BatchAccumulator accumulator = new BatchAccumulator(policy);

        assertTrue(accumulator.isEmpty());
        assertEquals(0, accumulator.getRecordCount());
        assertEquals(0, accumulator.getAccumulatedSizeBytes());
        assertNull(accumulator.getFirstRecordAt());
    }

    @Test
    void shouldAddRecord() {
        BatchPolicy policy = new BatchPolicy(
                100,
                1000,
                Duration.ofMinutes(1)
        );

        BatchAccumulator accumulator = new BatchAccumulator(policy);

        accumulator.add(createRecord(100));

        assertFalse(accumulator.isEmpty());
        assertEquals(1, accumulator.getRecordCount());
        assertEquals(100, accumulator.getAccumulatedSizeBytes());
        assertNotNull(accumulator.getFirstRecordAt());
    }

    @Test
    void shouldAccumulateRecordSizes() {
        BatchPolicy policy = new BatchPolicy(
                100,
                1000,
                Duration.ofMinutes(1)
        );

        BatchAccumulator accumulator = new BatchAccumulator(policy);

        accumulator.add(createRecord(100));
        accumulator.add(createRecord(250));
        accumulator.add(createRecord(50));

        assertEquals(3, accumulator.getRecordCount());
        assertEquals(400, accumulator.getAccumulatedSizeBytes());
    }

    @Test
    void shouldKeepFirstRecordTimestamp() {
        BatchPolicy policy = new BatchPolicy(
                100,
                1000,
                Duration.ofMinutes(1)
        );

        BatchAccumulator accumulator = new BatchAccumulator(policy);

        accumulator.add(createRecord(100));

        Instant firstTimestamp = accumulator.getFirstRecordAt();

        accumulator.add(createRecord(100));

        assertEquals(
                firstTimestamp,
                accumulator.getFirstRecordAt()
        );
    }

    @Test
    void shouldNotFlushEmptyBatch() {
        BatchPolicy policy = new BatchPolicy(
                2,
                1000,
                Duration.ofMinutes(1)
        );

        BatchAccumulator accumulator = new BatchAccumulator(policy);

        assertFalse(accumulator.shouldFlush());
    }

    @Test
    void shouldFlushWhenRecordCountReached() {
        BatchPolicy policy = new BatchPolicy(
                2,
                1000,
                Duration.ofMinutes(1)
        );

        BatchAccumulator accumulator = new BatchAccumulator(policy);

        accumulator.add(createRecord(100));
        assertFalse(accumulator.shouldFlush());

        accumulator.add(createRecord(100));

        assertTrue(accumulator.shouldFlush());
    }

    @Test
    void shouldFlushWhenSizeReached() {
        BatchPolicy policy = new BatchPolicy(
                100,
                500,
                Duration.ofMinutes(1)
        );

        BatchAccumulator accumulator = new BatchAccumulator(policy);

        accumulator.add(createRecord(300));
        assertFalse(accumulator.shouldFlush());

        accumulator.add(createRecord(200));

        assertTrue(accumulator.shouldFlush());
    }

    @Test
    void peekBatchShouldNotClearAccumulator() {
        BatchPolicy policy = new BatchPolicy(
                100,
                1000,
                Duration.ofMinutes(1)
        );

        BatchAccumulator accumulator = new BatchAccumulator(policy);

        accumulator.add(createRecord(100));
        accumulator.add(createRecord(200));

        RecordBatch batch = accumulator.peekBatch();

        assertNotNull(batch);

        assertEquals(2, accumulator.getRecordCount());
        assertEquals(300, accumulator.getAccumulatedSizeBytes());
        assertFalse(accumulator.isEmpty());
    }

    @Test
    void shouldClearAccumulator() {
        BatchPolicy policy = new BatchPolicy(
                100,
                1000,
                Duration.ofMinutes(1)
        );

        BatchAccumulator accumulator = new BatchAccumulator(policy);

        accumulator.add(createRecord(100));
        accumulator.add(createRecord(200));

        accumulator.clear();

        assertTrue(accumulator.isEmpty());
        assertEquals(0, accumulator.getRecordCount());
        assertEquals(0, accumulator.getAccumulatedSizeBytes());
        assertNull(accumulator.getFirstRecordAt());
    }

    @Test
    void clearingEmptyAccumulatorShouldDoNothing() {
        BatchPolicy policy = new BatchPolicy(
                100,
                1000,
                Duration.ofMinutes(1)
        );

        BatchAccumulator accumulator = new BatchAccumulator(policy);

        assertDoesNotThrow(accumulator::clear);

        assertTrue(accumulator.isEmpty());
    }

    @Test
    void shouldRejectNullPolicy() {
        assertThrows(
                NullPointerException.class,
                () -> new BatchAccumulator(null)
        );
    }

    @Test
    void shouldRejectNullRecord() {
        BatchPolicy policy = new BatchPolicy(
                100,
                1000,
                Duration.ofMinutes(1)
        );

        BatchAccumulator accumulator = new BatchAccumulator(policy);

        assertThrows(
                NullPointerException.class,
                () -> accumulator.add(null)
        );
    }

    @Test
    void peekBatchShouldFailWhenEmpty() {
        BatchPolicy policy = new BatchPolicy(
                100,
                1000,
                Duration.ofMinutes(1)
        );

        BatchAccumulator accumulator = new BatchAccumulator(policy);

        assertThrows(
                IllegalStateException.class,
                accumulator::peekBatch
        );
    }

    @Test
    void shouldTrackPendingOffsetsForSinglePartition() {
        BatchPolicy policy = new BatchPolicy(100, 1000, Duration.ofMinutes(1));
        BatchAccumulator accumulator = new BatchAccumulator(policy);

        accumulator.add(createRecord("netflow", 0, 10L, 100));

        Map<TopicPartitionKey, Long> offsets = accumulator.getPendingOffsets();

        assertEquals(1, offsets.size());
        assertEquals(11L, offsets.get(new TopicPartitionKey("netflow", 0)));
    }

    @Test
    void shouldTrackHighestOffsetForSamePartition() {
        BatchPolicy policy = new BatchPolicy(100, 1000, Duration.ofMinutes(1));
        BatchAccumulator accumulator = new BatchAccumulator(policy);

        accumulator.add(createRecord("netflow", 0, 10L, 100));
        accumulator.add(createRecord("netflow", 0, 11L, 100));
        accumulator.add(createRecord("netflow", 0, 15L, 100));

        Map<TopicPartitionKey, Long> offsets = accumulator.getPendingOffsets();

        assertEquals(1, offsets.size());
        assertEquals(16L, offsets.get(new TopicPartitionKey("netflow", 0)));
    }

    @Test
    void shouldTrackOffsetsForMultiplePartitions() {
        BatchPolicy policy = new BatchPolicy(100, 1000, Duration.ofMinutes(1));
        BatchAccumulator accumulator = new BatchAccumulator(policy);

        accumulator.add(createRecord("netflow", 0, 10L, 100));
        accumulator.add(createRecord("netflow", 1, 20L, 100));
        accumulator.add(createRecord("netflow", 2, 30L, 100));

        Map<TopicPartitionKey, Long> offsets = accumulator.getPendingOffsets();

        assertEquals(3, offsets.size());
        assertEquals(11L, offsets.get(new TopicPartitionKey("netflow", 0)));
        assertEquals(21L, offsets.get(new TopicPartitionKey("netflow", 1)));
        assertEquals(31L, offsets.get(new TopicPartitionKey("netflow", 2)));
    }

    @Test
    void shouldTrackOffsetsForDifferentTopics() {
        BatchPolicy policy = new BatchPolicy(100, 1000, Duration.ofMinutes(1));
        BatchAccumulator accumulator = new BatchAccumulator(policy);

        accumulator.add(createRecord("topic-a", 0, 10L, 100));
        accumulator.add(createRecord("topic-b", 0, 20L, 100));

        Map<TopicPartitionKey, Long> offsets = accumulator.getPendingOffsets();

        assertEquals(2, offsets.size());
        assertEquals(11L, offsets.get(new TopicPartitionKey("topic-a", 0)));
        assertEquals(21L, offsets.get(new TopicPartitionKey("topic-b", 0)));
    }

    @Test
    void shouldReturnUnmodifiablePendingOffsetsMap() {
        BatchPolicy policy = new BatchPolicy(100, 1000, Duration.ofMinutes(1));
        BatchAccumulator accumulator = new BatchAccumulator(policy);

        accumulator.add(createRecord("netflow", 0, 10L, 100));

        Map<TopicPartitionKey, Long> offsets = accumulator.getPendingOffsets();

        assertThrows(
                UnsupportedOperationException.class,
                () -> offsets.clear()
        );
    }
}

