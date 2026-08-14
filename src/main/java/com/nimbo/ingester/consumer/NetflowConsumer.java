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
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class NetflowConsumer {

    private static final Logger log = LoggerFactory.getLogger(NetflowConsumer.class);

    private final KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private final KafkaProperties kafkaProperties;
    private final AvroDeserializer avroDeserializer;
    private final DataProcessingService dataProcessingService;
    private final NetflowValidationService validationService;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final DlqService dlqService;
    private Thread consumerThread;


    public NetflowConsumer(
            KafkaConsumer<byte[], byte[]> kafkaConsumer,
            KafkaProperties kafkaProperties,
            AvroDeserializer avroDeserializer,
            DataProcessingService dataProcessingService,
            NetflowValidationService validationService, DlqService dlqService
    ) {
        this.kafkaConsumer = kafkaConsumer;
        this.kafkaProperties = kafkaProperties;
        this.avroDeserializer = avroDeserializer;
        this.dataProcessingService = dataProcessingService;
        this.validationService = validationService;
        this.dlqService = dlqService;

        registerRebalanceListener();
    }
    private void registerRebalanceListener() {
        this.kafkaConsumer.subscribe(
                Collections.singletonList(kafkaProperties.getTopic()),
                new ConsumerRebalanceListener() {
                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                        log.info("Partitions revoked: {}. Flushing buffered records...", partitions);
                        // Flush before partitions are revoked to prevent data loss
                        try {
                            Map<TopicPartitionKey, Long> flushedOffsets = dataProcessingService.flush();
                            if (!flushedOffsets.isEmpty()) {
                                Map<TopicPartition, OffsetAndMetadata> commitMap = new HashMap<>();
                                flushedOffsets.forEach((key, offset) -> {
                                    TopicPartition partition = new TopicPartition(key.topic(), key.partition());
                                    commitMap.put(partition, new OffsetAndMetadata(offset));
                                });
                                kafkaConsumer.commitSync(commitMap);
                                log.info("Flushed and committed {} offsets during rebalance", flushedOffsets.size());
                            }
                        } catch (Exception e) {
                            log.error("Failed to flush during rebalance", e);
                        }
                    }

                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                        log.info("Partitions assigned: {}", partitions);
                    }
                }
        );
    }

    @PostConstruct
    public void start() {
        log.info("Starting Netflow consumer for topic: {}", kafkaProperties.getTopic());
        this.consumerThread = new Thread(this::consumeLoop, "netflow-consumer-thread");
        this.consumerThread.setDaemon(true);
        this.consumerThread.start();
    }

    private void consumeLoop() {
        try {
            kafkaConsumer.subscribe(Collections.singletonList(kafkaProperties.getTopic()));
            log.info("Subscribed to topic: {}", kafkaProperties.getTopic());

            while (running.get()) {
                try {
                    ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(
                            Duration.ofMillis(kafkaProperties.getPollTimeoutMs())
                    );

                    if (records.isEmpty()) {
                        System.out.println(
                                "No records received..."
                        );
                        continue;
                    }

                    Map<TopicPartitionKey, Long> offsetsToCommit = new HashMap<>();

                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        Optional<Map<TopicPartitionKey, Long>> result = processRecord(record);
                        result.ifPresent(offsets -> {
                            offsets.forEach((key, offset) -> {
                                offsetsToCommit.merge(key, offset, Math::max);
                            });
                        });
                    }

                    if (!kafkaProperties.isEnableAutoCommit() && !offsetsToCommit.isEmpty()) {
                        Map<TopicPartition, OffsetAndMetadata> commitMap = new HashMap<>();
                        offsetsToCommit.forEach((key, offset) -> {
                            TopicPartition partition = new TopicPartition(key.topic(), key.partition());
                            commitMap.put(partition, new OffsetAndMetadata(offset));
                        });
                        kafkaConsumer.commitSync(commitMap);
                        log.debug("Committed offsets: {}", offsetsToCommit);
                    }

                } catch (Exception e) {
                    log.error("Error during Kafka consumption", e);
                }
            }

        } catch (Exception e) {
            log.error("Fatal error in consumer loop", e);
            throw new KafkaConsumerException("Consumer loop failed", e);
        } finally {
            try {
                kafkaConsumer.close();
                log.info("Kafka consumer closed");
            } catch (Exception e) {
                log.warn("Error closing Kafka consumer", e);
            }
        }
    }

    private Optional<Map<TopicPartitionKey, Long>> processRecord(
            ConsumerRecord<byte[], byte[]> record
    ) {

        try {

            var genericRecord =
                    avroDeserializer.deserialize(
                            record.value()
                    );


            ValidationResult result =
                    validationService.validate(
                            genericRecord
                    );


            if (!result.valid()) {

//                log.warn(
//                        "Invalid Netflow record: topic={}, partition={}, offset={}, error={}",
//                        record.topic(),
//                        record.partition(),
//                        record.offset(),
//                        result.errorMessage()
//                );

                dlqService.publish(
                        record,
                        "VALIDATION_ERROR",
                        result.errorMessage()
                );

                return Optional.empty();
            }


            IngestedRecord ingestedRecord =
                    new IngestedRecord(
                            genericRecord,
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.value().length
                    );


            return dataProcessingService
                    .processRecord(
                            ingestedRecord
                    );


        } catch (DeserializationException e) {

            dlqService.publish(
                    record,
                    "DESERIALIZATION_ERROR",
                    e.getMessage()
            );
            return Optional.empty();


        } catch (Exception e) {

            log.error(
                    "Error processing record: topic={}, partition={}, offset={}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    e
            );

            return Optional.empty();
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down Netflow consumer...");
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
                log.info("Flushed and committed {} offsets during shutdown", flushedOffsets.size());
            }
        } catch (Exception e) {
            log.error("Error flushing during shutdown", e);
        }

        if (consumerThread != null) {
            try {
                consumerThread.interrupt();
                consumerThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}