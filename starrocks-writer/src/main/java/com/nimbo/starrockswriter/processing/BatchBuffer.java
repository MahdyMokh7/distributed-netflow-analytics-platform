package com.nimbo.starrockswriter.processing;

import com.nimbo.starrockswriter.config.StarRocksProperties;
import com.nimbo.starrockswriter.model.StarRocksRow;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

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
    if (reachedSize()) {
      return FlushReason.SIZE;
    }
    if (reachedTime()) {
      return FlushReason.TIME;
    }
    return null;
  }

  private boolean reachedSize() {
    return rows.size() >= properties.getMaxBatchSizeRows();
  }

  private boolean reachedTime() {
    if (isEmpty()) {
      return false;
    }
    return System.currentTimeMillis() - lastFlushTime >= properties.getMaxBatchTimeMs();
  }

  public boolean isEmpty() {
    return rows.isEmpty() && offsets.isEmpty();
  }

  public int rowCount() {
    return rows.size();
  }

  public long ageSeconds() {
    return isEmpty() ? 0 : (System.currentTimeMillis() - lastFlushTime) / 1000;
  }

  public BatchSnapshot snapshot() {
    return new BatchSnapshot(List.copyOf(rows), Map.copyOf(offsets));
  }

  public void clear() {
    rows.clear();
    offsets.clear();
    lastFlushTime = System.currentTimeMillis();
  }

  public enum FlushReason {
    SIZE,
    TIME,
    REBALANCE,
    SHUTDOWN,
    DESERIALIZATION_ERROR
  }

  public record BatchSnapshot(
      List<StarRocksRow> rows, Map<TopicPartition, OffsetAndMetadata> offsets) {

  }
}
