package com.nimbo.ingester.service;

import com.nimbo.ingester.model.IngestedRecord;
import com.nimbo.storage.model.TopicPartitionKey;
import com.nimbo.storage.service.NetFlowStorage;
import java.util.Map;
import java.util.Optional;

public class DataProcessingService {

  private final NetFlowStorage netFlowStorage;

  public DataProcessingService(NetFlowStorage netFlowStorage) {
    this.netFlowStorage = netFlowStorage;
  }

  public Optional<Map<TopicPartitionKey, Long>> processRecord(IngestedRecord ingestedRecord) {

    return netFlowStorage.accept(ingestedRecord);
  }

  public boolean isWithinAllowedWindow(long kafkaTimestamp) {
    return netFlowStorage.isWithinAllowedWindow(kafkaTimestamp);
  }

  // rebalance/shutdown
  public Map<TopicPartitionKey, Long> flush() {
    return netFlowStorage.flush();
  }

  public Map<TopicPartitionKey, Long> flushRemaining() {
    return netFlowStorage.flushRemaining();
  }

  public Optional<Map<TopicPartitionKey, Long>> recordSkippedOffset(
      String topic, int partition, long nextOffset) {

    return netFlowStorage.recordSkippedOffset(topic, partition, nextOffset);
  }
}
