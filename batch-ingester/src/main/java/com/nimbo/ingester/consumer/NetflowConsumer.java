package com.nimbo.ingester.consumer;

import com.nimbo.ingester.config.KafkaProperties;
import com.nimbo.ingester.deserializer.AvroDeserializer;
import com.nimbo.ingester.exception.DeserializationException;
import com.nimbo.ingester.exception.KafkaConsumerException;
import com.nimbo.ingester.model.IngestedRecord;
import com.nimbo.ingester.model.ValidationResult;
import com.nimbo.ingester.service.DataProcessingService;
import com.nimbo.ingester.service.DlqService;
import com.nimbo.ingester.service.NetflowValidationService;
import com.nimbo.storage.model.TopicPartitionKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class NetflowConsumer {

    private final KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private final KafkaProperties kafkaProperties;
    private final AvroDeserializer avroDeserializer;
    private final DataProcessingService dataProcessingService;
    private final NetflowValidationService validationService;
    private final DlqService dlqService;
    private final int instanceId;
    private final KafkaOffsetCommitter kafkaOffsetCommitter;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private Thread consumerThread;

    public NetflowConsumer(
            KafkaConsumer<byte[], byte[]> kafkaConsumer,
            KafkaProperties kafkaProperties,
            AvroDeserializer avroDeserializer,
            DataProcessingService dataProcessingService,
            NetflowValidationService validationService,
            DlqService dlqService,
            KafkaOffsetCommitter kafkaOffsetCommitter,
            int instanceId) {
        this.kafkaConsumer = kafkaConsumer;
        this.kafkaProperties = kafkaProperties;
        this.avroDeserializer = avroDeserializer;
        this.dataProcessingService = dataProcessingService;
        this.validationService = validationService;
        this.dlqService = dlqService;
        this.kafkaOffsetCommitter = kafkaOffsetCommitter;
        this.instanceId = instanceId;
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
                            log.info("Instance #{}: Flushed {} offsets during rebalance",
                                    instanceId, flushedOffsets.size());
                        } catch (Exception e) {
                            log.error("Instance #{}: Failed to flush during rebalance", instanceId, e);
                        }
                    }

                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                        log.info("Instance #{}: Partitions assigned: {}", instanceId, partitions);
                    }
                }
        );
    }

    public void start() {
        log.info("Starting consumer instance #{} for topic: {}",
                instanceId, kafkaProperties.getTopic());
        this.consumerThread = new Thread(this::consumeLoop,
                "consumer-instance-" + instanceId);
        this.consumerThread.setDaemon(true);
        this.consumerThread.start();
    }

    private void consumeLoop() {
        try {
            registerRebalanceListener();
            log.info("Instance #{}: Subscribed to topic: {}",
                    instanceId, kafkaProperties.getTopic());

            while (running.get()) {
                consumePoll();
            }

        } catch (Exception e) {
            log.error("Instance #{}: Fatal error in consumer loop", instanceId, e);
            throw new KafkaConsumerException("Consumer loop failed", e);
        } finally {
            closeConsumer();
        }
    }

    private void consumePoll() {
        try {
            ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(
                    Duration.ofMillis(kafkaProperties.getPollTimeoutMs())
            );

            if (records.isEmpty()) {
                handleEmptyPoll();
                return;
            }

            Map<TopicPartitionKey, Long> offsetsToCommit = processRecords(records);
            commitOffsets(offsetsToCommit);

        } catch (Exception e) {
            log.error("Instance #{}: Error during Kafka consumption", instanceId, e);
        }
    }

    private void handleEmptyPoll() {
        log.debug("No records received from Kafka");

        Map<TopicPartitionKey, Long> offsetsToCommit =
                dataProcessingService.flushRemaining();

        commitOffsets(offsetsToCommit);
    }

    private Map<TopicPartitionKey, Long> processRecords(
            ConsumerRecords<byte[], byte[]> records) {

        Map<TopicPartitionKey, Long> offsetsToCommit = new HashMap<>();

        for (ConsumerRecord<byte[], byte[]> consumerRecord : records) {
            processRecord(consumerRecord).ifPresent(offsets ->
                    offsets.forEach((key, offset) ->
                            offsetsToCommit.merge(key, offset, Math::max)));
        }

        return offsetsToCommit;
    }

    private Optional<Map<TopicPartitionKey, Long>> processRecord(ConsumerRecord<byte[], byte[]> consumerRecord) {
        long nextOffset = consumerRecord.offset() + 1;
        try {
            var genericRecord = avroDeserializer.deserialize(consumerRecord.value());
            ValidationResult result = validationService.validate(genericRecord);

            if (!result.valid()) {
                dlqService.publish(consumerRecord, "VALIDATION_ERROR", result.errorMessage());
                return dataProcessingService.recordSkippedOffset(
                        consumerRecord.topic(), consumerRecord.partition(), nextOffset);
            }

            IngestedRecord ingestedRecord = new IngestedRecord(
                    genericRecord,
                    consumerRecord.topic(),
                    consumerRecord.partition(),
                    consumerRecord.offset(),
                    consumerRecord.value().length
            );

            return dataProcessingService.processRecord(ingestedRecord);

        } catch (DeserializationException e) {
            dlqService.publish(consumerRecord, "DESERIALIZATION_ERROR", e.getMessage());
            return dataProcessingService.recordSkippedOffset(
                    consumerRecord.topic(), consumerRecord.partition(), nextOffset);
        } catch (Exception e) {
            log.error("Instance #{}: Error processing record: topic={}, partition={}, offset={}",
                    instanceId, consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), e);
            return Optional.empty();
        }
    }

    private void commitOffsets(Map<TopicPartitionKey, Long> offsetsToCommit) {
        if (kafkaProperties.isEnableAutoCommit() || offsetsToCommit.isEmpty()) {
            return;
        }

        kafkaOffsetCommitter.commit(kafkaConsumer, offsetsToCommit);

        log.debug("Instance #{}: Committed offsets: {}",
                instanceId, offsetsToCommit);
    }

    private void closeConsumer() {
        try {
            kafkaConsumer.close();
            log.info("Instance #{}: Kafka consumer closed", instanceId);
        } catch (Exception e) {
            log.warn("Instance #{}: Error closing Kafka consumer", instanceId, e);
        }
    }

    public void shutdown() {
        log.info("Instance #{}: Shutting down...", instanceId);
        running.set(false);

        try {
            log.info("Flushing remaining buffered records before shutdown...");
            Map<TopicPartitionKey, Long> flushedOffsets = dataProcessingService.flush();
            if (!flushedOffsets.isEmpty()) {
                kafkaOffsetCommitter.commit(kafkaConsumer, flushedOffsets);
                log.info("Instance #{}: Flushed {} offsets during shutdown",
                        instanceId, flushedOffsets.size());
            }
        } catch (Exception e) {
            log.error("Instance #{}: Error flushing during shutdown", instanceId, e);
        }

        if (consumerThread != null) {
            try {
                consumerThread.interrupt();
                consumerThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        log.info("Instance #{}: Shutdown complete", instanceId);
    }
}