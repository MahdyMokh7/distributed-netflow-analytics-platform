package com.nimbo.starrockswriter.processing;

import com.nimbo.starrockswriter.config.StarRocksProperties;
import com.nimbo.starrockswriter.model.StarRocksRow;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class BatchBufferTest {

    private StarRocksProperties properties(int maxRows, long maxTimeMs) {
        StarRocksProperties properties = new StarRocksProperties();
        properties.setMaxBatchSizeRows(maxRows);
        properties.setMaxBatchTimeMs(maxTimeMs);
        return properties;
    }

    private StarRocksRow row() {
        return new StarRocksRow("topic", 0, 1L, 100L, Map.of("value", "value"));
    }

    @Test
    void shouldStartEmpty() {
        BatchBuffer buffer = new BatchBuffer(properties(10, 10000));

        assertThat(buffer.isEmpty()).isTrue();
    }

    @Test
    void shouldAddRowAndOffset() {
        BatchBuffer buffer = new BatchBuffer(properties(10, 10000));
        TopicPartition partition = new TopicPartition("topic", 0);
        OffsetAndMetadata offset = new OffsetAndMetadata(10);

        buffer.add(row(), partition, offset);

        BatchBuffer.BatchSnapshot snapshot = buffer.snapshot();

        assertThat(snapshot.rows()).containsExactly(row());
        assertThat(snapshot.offsets()).containsEntry(partition, offset);
    }

    @Test
    void shouldAddOffsetWithoutRow() {
        BatchBuffer buffer = new BatchBuffer(properties(10, 10000));
        TopicPartition partition = new TopicPartition("topic", 0);
        OffsetAndMetadata offset = new OffsetAndMetadata(20);

        buffer.addOffset(partition, offset);

        BatchBuffer.BatchSnapshot snapshot = buffer.snapshot();

        assertThat(snapshot.rows()).isEmpty();
        assertThat(snapshot.offsets()).containsEntry(partition, offset);
        assertThat(buffer.isEmpty()).isFalse();
    }

    @Test
    void shouldFlushWhenRowLimitReached() {
        BatchBuffer buffer = new BatchBuffer(properties(1, 100000));

        buffer.add(row(), new TopicPartition("topic", 0), new OffsetAndMetadata(1));

        assertThat(buffer.shouldFlush()).isTrue();
    }

    @Test
    void shouldNotFlushEmptyBufferBecauseOfTime() {
        BatchBuffer buffer = new BatchBuffer(properties(100, 0));

        assertThat(buffer.shouldFlush()).isFalse();
    }

    @Test
    void shouldCreateImmutableSnapshot() {
        BatchBuffer buffer = new BatchBuffer(properties(10, 10000));

        buffer.add(row(), new TopicPartition("topic", 0), new OffsetAndMetadata(1));

        BatchBuffer.BatchSnapshot snapshot = buffer.snapshot();

        assertThat(snapshot.rows()).hasSize(1);
        assertThat(snapshot.offsets()).hasSize(1);
    }

    @Test
    void shouldClearEverything() {
        BatchBuffer buffer = new BatchBuffer(properties(10, 10000));

        buffer.add(row(), new TopicPartition("topic", 0), new OffsetAndMetadata(1));

        buffer.clear();

        assertThat(buffer.isEmpty()).isTrue();

        BatchBuffer.BatchSnapshot snapshot = buffer.snapshot();

        assertThat(snapshot.rows()).isEmpty();
        assertThat(snapshot.offsets()).isEmpty();
    }
}
