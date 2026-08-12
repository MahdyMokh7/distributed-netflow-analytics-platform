package com.nimbo.ingester.consumer;

import com.nimbo.ingester.config.KafkaProperties;
import com.nimbo.ingester.deserializer.AvroDeserializer;
import com.nimbo.ingester.exception.KafkaConsumerException;
import com.nimbo.ingester.model.IngestedRecord;
import com.nimbo.ingester.service.DataProcessingService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class NetflowConsumer {

    private static final Logger log = LoggerFactory.getLogger(NetflowConsumer.class);

    private final KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private final KafkaProperties kafkaProperties;
    private final AvroDeserializer avroDeserializer;
    private final DataProcessingService dataProcessingService;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private Thread consumerThread;

    public NetflowConsumer(
            KafkaConsumer<byte[], byte[]> kafkaConsumer,
            KafkaProperties kafkaProperties,
            AvroDeserializer avroDeserializer,
            DataProcessingService dataProcessingService) {
        this.kafkaConsumer = kafkaConsumer;
        this.kafkaProperties = kafkaProperties;
        this.avroDeserializer = avroDeserializer;
        this.dataProcessingService = dataProcessingService;
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
                        continue;
                    }

                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        processRecord(record);
                    }

                    if (!kafkaProperties.isEnableAutoCommit()) {
                        kafkaConsumer.commitSync();
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

    private void processRecord(ConsumerRecord<byte[], byte[]> record) {
        try {
            var genericRecord = avroDeserializer.deserialize(record.value());

            IngestedRecord ingestedRecord = new IngestedRecord(
                    genericRecord,
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.value().length
            );

            dataProcessingService.processRecord(ingestedRecord);

        } catch (Exception e) {
            log.error("Error processing record: topic={}, partition={}, offset={}",
                    record.topic(), record.partition(), record.offset(), e);
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down Netflow consumer...");
        running.set(false);
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