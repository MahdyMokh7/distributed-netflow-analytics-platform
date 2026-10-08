package com.nimbo.ingestion;

import com.nimbo.model.IngestedRecord;
import com.nimbo.model.TopicPartitionKey;
import com.nimbo.storage.NetFlowParquetWriter;
import java.util.Map;
import java.util.Optional;

public class DataProcessingService {

  private final NetFlowParquetWriter netFlowParquetWriter;

  public DataProcessingService(NetFlowParquetWriter netFlowParquetWriter) {
    this.netFlowParquetWriter = netFlowParquetWriter;
  }

  public Optional<Map<TopicPartitionKey, Long>> processRecord(IngestedRecord ingestedRecord) {

    return netFlowParquetWriter.accept(ingestedRecord);
  }

  public boolean isWithinAllowedWindow(long kafkaTimestamp) {
    return netFlowParquetWriter.isWithinAllowedWindow(kafkaTimestamp);
  }

  // rebalance/shutdown
  public Map<TopicPartitionKey, Long> flush() {
    return netFlowParquetWriter.flush();
  }

  public Map<TopicPartitionKey, Long> flushRemaining() {
    return netFlowParquetWriter.flushRemaining();
  }

  public Optional<Map<TopicPartitionKey, Long>> recordSkippedOffset(
      String topic, int partition, long nextOffset) {

    return netFlowParquetWriter.recordSkippedOffset(topic, partition, nextOffset);
  }
}
