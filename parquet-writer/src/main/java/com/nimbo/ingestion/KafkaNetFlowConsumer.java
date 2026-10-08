package com.nimbo.ingestion;

import com.nimbo.common.kafka.KafkaDlqPublisher.PublishException;
import com.nimbo.config.KafkaProperties;
import com.nimbo.ingestion.exception.KafkaConsumerException;
import com.nimbo.ingestion.exception.SchemaRegistryException;
import com.nimbo.metrics.ParquetWriterMetrics;
import com.nimbo.model.TopicPartitionKey;
import com.nimbo.storage.exception.StorageException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

@Slf4j(topic = "com.nimbo.ingestion.NetflowConsumer")
public class KafkaNetFlowConsumer {

  private final KafkaConsumer<byte[], byte[]> kafkaConsumer;
  private final KafkaProperties kafkaProperties;
  private final AvroDeserializer avroDeserializer;
  private final DataProcessingService dataProcessingService;
  private final NetflowRecordProcessor recordProcessor;
  private final int instanceId;
  private final KafkaOffsetCommitter kafkaOffsetCommitter;

  private final AtomicBoolean running = new AtomicBoolean(true);
  private Thread consumerThread;
  private final ParquetWriterMetrics metrics;

  public KafkaNetFlowConsumer(
      KafkaConsumer<byte[], byte[]> kafkaConsumer,
      KafkaProperties kafkaProperties,
      AvroDeserializer avroDeserializer,
      DataProcessingService dataProcessingService,
      NetflowRecordProcessor recordProcessor,
      KafkaOffsetCommitter kafkaOffsetCommitter,
      int instanceId,
      ParquetWriterMetrics metrics) {
    this.kafkaConsumer = kafkaConsumer;
    this.kafkaProperties = kafkaProperties;
    this.avroDeserializer = avroDeserializer;
    this.dataProcessingService = dataProcessingService;
    this.recordProcessor = recordProcessor;
    this.kafkaOffsetCommitter = kafkaOffsetCommitter;
    this.instanceId = instanceId;
    this.metrics = metrics;
  }

  private void registerRebalanceListener() {
    this.kafkaConsumer.subscribe(
        Collections.singletonList(kafkaProperties.getTopic()),
        new ConsumerRebalanceListener() {
          @Override
          public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.info("Instance #{}: Partitions revoked: {}", instanceId, partitions);
            try {
              Map<TopicPartitionKey, Long> flushedOffsets = dataProcessingService.flush();
              kafkaOffsetCommitter.commit(kafkaConsumer, flushedOffsets);
              log.info(
                  "Instance #{}: Flushed {} offsets during rebalance",
                  instanceId,
                  flushedOffsets.size());
            } catch (StorageException | KafkaException e) {
              log.error("Instance #{}: Failed to flush during rebalance", instanceId, e);
            }
          }

          @Override
          public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log.info("Instance #{}: Partitions assigned: {}", instanceId, partitions);
          }
        });
  }

  public void start() {
    log.info("Starting consumer instance #{} for topic: {}", instanceId, kafkaProperties.getTopic());
    this.consumerThread = new Thread(this::consumeLoop, "consumer-instance-" + instanceId);
    this.consumerThread.setDaemon(true);
    this.consumerThread.start();
  }

  private void consumeLoop() {
    try {
      registerRebalanceListener();
      metrics.consumerStarted(instanceId);
      log.info("Instance #{}: Subscribed to topic: {}", instanceId, kafkaProperties.getTopic());

      while (running.get()) {
        consumePoll();
      }

    } catch (KafkaException | IllegalArgumentException | IllegalStateException e) {
      log.error("Instance #{}: Fatal error in consumer loop", instanceId, e);
      throw new KafkaConsumerException("Consumer loop failed", e);
    } finally {
      flushAndCommitOnShutdown();
      metrics.consumerStopped(instanceId);
      closeConsumer();
    }
  }

  private void consumePoll() {
    try {

      ConsumerRecords<byte[], byte[]> records =
          kafkaConsumer.poll(Duration.ofMillis(kafkaProperties.getPollTimeoutMs()));

      metrics.recordsPolled(instanceId, records.count());
      if (records.isEmpty()) {
        handleEmptyPoll();
        return;
      }

      Map<TopicPartitionKey, Long> offsetsToCommit = processRecords(records);
      commitOffsets(offsetsToCommit);
    } catch (WakeupException e) {
      log.info("Consumer {} shutting down", instanceId);
    } catch (KafkaException e) {

      log.error("Instance #{}: Kafka error during polling or offset commit", instanceId, e);
      throw new KafkaConsumerException(
          "Kafka polling failed for consumer instance #" + instanceId, e);

    } catch (StorageException e) {
      log.error("Instance #{}: Error while processing or flushing consumed records", instanceId, e);
      rewindToCommittedOffsets();
    } catch (IllegalStateException e) {
      log.error("Instance #{}: Error while processing or flushing consumed records", instanceId, e);
    }
  }

  private void rewindToCommittedOffsets() {
    Set<TopicPartition> partitions = kafkaConsumer.assignment();
    if (partitions.isEmpty()) {
      return;
    }

    Map<TopicPartition, OffsetAndMetadata> committedOffsets = kafkaConsumer.committed(partitions);
    Set<TopicPartition> partitionsWithoutCommit = new HashSet<>();

    for (TopicPartition partition : partitions) {
      OffsetAndMetadata committedOffset = committedOffsets.get(partition);
      if (committedOffset == null) {
        partitionsWithoutCommit.add(partition);
      } else {
        kafkaConsumer.seek(partition, committedOffset.offset());
      }
    }
    if (!partitionsWithoutCommit.isEmpty()) {
      kafkaConsumer.seekToBeginning(partitionsWithoutCommit);
    }

    log.warn("Instance #{}: Rewound assigned partitions after storage failure", instanceId);
  }

  private void handleEmptyPoll() {
    log.debug("No records received from Kafka");

    Map<TopicPartitionKey, Long> offsetsToCommit = dataProcessingService.flushRemaining();

    commitOffsets(offsetsToCommit);
  }

  private Map<TopicPartitionKey, Long> processRecords(ConsumerRecords<byte[], byte[]> records) {

    Map<TopicPartitionKey, Long> offsetsToCommit = new HashMap<>();
    Set<TopicPartition> blockedPartitions = new HashSet<>();

    for (ConsumerRecord<byte[], byte[]> consumerRecord : records) {
      TopicPartition topicPartition =
          new TopicPartition(consumerRecord.topic(), consumerRecord.partition());

      if (blockedPartitions.contains(topicPartition)) {
        continue;
      }

      try {

        recordProcessor.processRecord(consumerRecord)
            .ifPresent(
                offsets ->
                    offsets.forEach(
                        (key, offset) -> offsetsToCommit.merge(key, offset, Math::max)));

      } catch (SchemaRegistryException | PublishException exception) {

        blockedPartitions.add(topicPartition);
        kafkaConsumer.seek(topicPartition, consumerRecord.offset());
        log.error(
            "Instance #{}: Retryable dependency failed for topic={}, partition={}, offset={}; "
                + "the record will be retried",
            instanceId,
            consumerRecord.topic(),
            consumerRecord.partition(),
            consumerRecord.offset(),
            exception);
      }
    }

    return offsetsToCommit;
  }

  private void commitOffsets(Map<TopicPartitionKey, Long> offsetsToCommit) {
    if (kafkaProperties.isEnableAutoCommit() || offsetsToCommit.isEmpty()) {
      return;
    }

    kafkaOffsetCommitter.commit(kafkaConsumer, offsetsToCommit);

    log.debug("Instance #{}: Committed offsets: {}", instanceId, offsetsToCommit);
  }

  private void closeConsumer() {
    try {
      kafkaConsumer.close();
      log.info("Instance #{}: Kafka consumer closed", instanceId);
    } catch (KafkaException | IllegalStateException e) {
      log.warn("Instance #{}: Error closing Kafka consumer", instanceId, e);
    }

    try {
      avroDeserializer.close();
      log.info("Instance #{}: Avro deserializer closed", instanceId);
    } catch (RuntimeException e) {
      log.warn("Instance #{}: Error closing Avro deserializer", instanceId, e);
    }
  }

  private void flushAndCommitOnShutdown() {

    log.info("Instance #{}: Flushing remaining buffered records before shutdown...", instanceId);

    try {
      Map<TopicPartitionKey, Long> flushedOffsets = dataProcessingService.flush();

      if (!flushedOffsets.isEmpty()) {
        kafkaOffsetCommitter.commit(kafkaConsumer, flushedOffsets);
        log.info(
            "Instance #{}: Flushed {} offsets during shutdown", instanceId, flushedOffsets.size());
      }

    } catch (StorageException | KafkaException | IllegalStateException e) {
      log.error("Instance #{}: Error flushing during shutdown", instanceId, e);
    }
  }

  public void shutdown() {
    log.info("Instance #{}: Shutting down...", instanceId);
    running.set(false);
    kafkaConsumer.wakeup();

    if (consumerThread != null) {
      try {
        consumerThread.join(5000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    log.info("Instance #{}: Shutdown complete", instanceId);
  }
}
