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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class NetflowConsumer {

    private final KafkaProperties kafkaProperties;
    private final AvroDeserializer avroDeserializer;
    private final DataProcessingService dataProcessingService;
    private final NetflowValidationService validationService;
    private final DlqService dlqService;
    private final int instanceId;

    private KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private Thread consumerThread;

    public NetflowConsumer(
            KafkaProperties kafkaProperties,
            AvroDeserializer avroDeserializer,
            DataProcessingService dataProcessingService,
            NetflowValidationService validationService,
            DlqService dlqService,
            int instanceId) {
        this.kafkaProperties = kafkaProperties;
        this.avroDeserializer = avroDeserializer;
        this.dataProcessingService = dataProcessingService;
        this.validationService = validationService;
        this.dlqService = dlqService;
        this.instanceId = instanceId;

        this.kafkaConsumer = createKafkaConsumer();
    }

    private KafkaConsumer<byte[], byte[]> createKafkaConsumer() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaProperties.getBootstrapServers());
        configs.put(ConsumerConfig.GROUP_ID_CONFIG,
                kafkaProperties.getConsumerGroup());
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                kafkaProperties.isEnableAutoCommit());
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest");
        configs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                kafkaProperties.getMaxPollRecords());
        configs.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,
                kafkaProperties.getSessionTimeoutMs());
        configs.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,
                kafkaProperties.getHeartbeatIntervalMs());
        configs.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
                kafkaProperties.getMaxPollIntervalMs());

        configs.put(ConsumerConfig.CLIENT_ID_CONFIG,
                "consumer-" + kafkaProperties.getConsumerGroup() + "-" + instanceId);

        if (kafkaProperties.getAdditionalConfigs() != null) {
            configs.putAll(kafkaProperties.getAdditionalConfigs());
        }

        return new KafkaConsumer<>(configs);
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
                            if (!flushedOffsets.isEmpty()) {
                                Map<TopicPartition, OffsetAndMetadata> commitMap = new HashMap<>();
                                flushedOffsets.forEach((key, offset) -> {
                                    TopicPartition partition = new TopicPartition(key.topic(), key.partition());
                                    commitMap.put(partition, new OffsetAndMetadata(offset));
                                });
                                kafkaConsumer.commitSync(commitMap);
                                log.info("Instance #{}: Flushed {} offsets during rebalance",
                                        instanceId, flushedOffsets.size());
                            }
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
                try {
                    ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(
                            Duration.ofMillis(kafkaProperties.getPollTimeoutMs())
                    );

                    if (records.isEmpty()) {
                        log.debug("No records received from Kafka");

                        Map<TopicPartitionKey, Long> offsetsToCommit =
                                dataProcessingService.flushIfNeeded();

                        if (!kafkaProperties.isEnableAutoCommit() && !offsetsToCommit.isEmpty()) {

                            Map<TopicPartition, OffsetAndMetadata> commitMap = new HashMap<>();

                            offsetsToCommit.forEach((key, offset) -> {
                                TopicPartition partition =
                                        new TopicPartition(key.topic(), key.partition());

                                commitMap.put(
                                        partition,
                                        new OffsetAndMetadata(offset)
                                );
                            });

                            kafkaConsumer.commitSync(commitMap);

                            log.debug(
                                    "Committed offsets for remaining records: {}",
                                    offsetsToCommit
                            );
                        }

                        continue;
                    }

                    Map<TopicPartitionKey, Long> offsetsToCommit = new HashMap<>();

                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        processRecord(record).ifPresent(offsets ->
                                offsets.forEach((key, offset) -> offsetsToCommit.merge(key, offset, Math::max))
                        );
                    }

                    if (!kafkaProperties.isEnableAutoCommit() && !offsetsToCommit.isEmpty()) {

                        Map<TopicPartition, OffsetAndMetadata> commitMap = new HashMap<>();

                        offsetsToCommit.forEach((key, offset) -> {
                            TopicPartition partition =
                                    new TopicPartition(key.topic(), key.partition());

                            commitMap.put(partition, new OffsetAndMetadata(offset)
                            );
                        });

                        kafkaConsumer.commitSync(commitMap);
                        log.debug("Instance #{}: Committed offsets: {}", instanceId, offsetsToCommit);
                    }

                } catch (Exception e) {
                    log.error("Instance #{}: Error during Kafka consumption", instanceId, e);
                }
            }

        } catch (Exception e) {
            log.error("Instance #{}: Fatal error in consumer loop", instanceId, e);
            throw new KafkaConsumerException("Consumer loop failed", e);
        } finally {
            try {
                kafkaConsumer.close();
                log.info("Instance #{}: Kafka consumer closed", instanceId);
            } catch (Exception e) {
                log.warn("Instance #{}: Error closing Kafka consumer", instanceId, e);
            }
        }
    }

    private Optional<Map<TopicPartitionKey, Long>> processRecord(ConsumerRecord<byte[], byte[]> record) {
        try {
            var genericRecord = avroDeserializer.deserialize(record.value());

            ValidationResult result = validationService.validate(genericRecord);

            if (!result.valid()) {
                dlqService.publish(record, "VALIDATION_ERROR", result.errorMessage());
                return Optional.empty();
            }

            IngestedRecord ingestedRecord = new IngestedRecord(
                    genericRecord,
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.value().length
            );

            return dataProcessingService.processRecord(ingestedRecord);

        } catch (DeserializationException e) {
            dlqService.publish(record, "DESERIALIZATION_ERROR", e.getMessage());
            return Optional.empty();
        } catch (Exception e) {
            log.error("Instance #{}: Error processing record: topic={}, partition={}, offset={}",
                    instanceId, record.topic(), record.partition(), record.offset(), e);
            return Optional.empty();
        }
    }

    public void shutdown() {
        log.info("Instance #{}: Shutting down...", instanceId);
        running.set(false);

        // SCENARIO 1: GRACEFUL SHUTDOWN - Flush remaining records
        try {
            log.info("Flushing remaining buffered records before shutdown...");
            Map<TopicPartitionKey, Long> flushedOffsets = dataProcessingService.flush();
            if (!flushedOffsets.isEmpty()) {
                Map<TopicPartition, OffsetAndMetadata> commitMap = new HashMap<>();
                flushedOffsets.forEach((key, offset) -> {
                    TopicPartition partition = new TopicPartition(key.topic(), key.partition());
                    commitMap.put(partition, new OffsetAndMetadata(offset));
                });
                kafkaConsumer.commitSync(commitMap);
                log.info("Instance #{}: Flushed {} offsets during shutdown", instanceId, flushedOffsets.size());
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