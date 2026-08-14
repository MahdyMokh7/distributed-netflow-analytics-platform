package com.nimbo.ingester.service;

import com.nimbo.ingester.config.KafkaProperties;
import com.nimbo.ingester.exception.DlqPublishException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

@Service
public class DlqService {

    private static final Logger log =
            LoggerFactory.getLogger(DlqService.class);

    private final KafkaProducer<byte[], byte[]> kafkaProducer;
    private final KafkaProperties kafkaProperties;

    public DlqService(
            KafkaProducer<byte[], byte[]> kafkaProducer,
            KafkaProperties kafkaProperties
    ) {
        this.kafkaProducer = kafkaProducer;
        this.kafkaProperties = kafkaProperties;
    }


    public void publish(
            ConsumerRecord<byte[], byte[]> originalRecord,
            String errorType,
            String errorMessage
    ) {

        ProducerRecord<byte[], byte[]> dlqRecord =
                new ProducerRecord<>(
                        kafkaProperties.getDlqTopic(),

                        originalRecord.key(),

                        originalRecord.value()
                );


        addHeader(
                dlqRecord,
                "original-topic",
                originalRecord.topic()
        );

        addHeader(
                dlqRecord,
                "original-partition",
                String.valueOf(
                        originalRecord.partition()
                )
        );

        addHeader(
                dlqRecord,
                "original-offset",
                String.valueOf(
                        originalRecord.offset()
                )
        );

        addHeader(
                dlqRecord,
                "error-type",
                errorType
        );

        addHeader(
                dlqRecord,
                "error-message",
                errorMessage
        );


        try {
            kafkaProducer
                    .send(dlqRecord)
                    .get();


//            log.warn(
//                    "Record published to DLQ: topic={}, partition={}, offset={}, errorType={}",
//                    originalRecord.topic(),
//                    originalRecord.partition(),
//                    originalRecord.offset(),
//                    errorType
//            );


        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();

            throw new DlqPublishException(
                    "Interrupted while publishing record to DLQ",
                    e
            );


        } catch (ExecutionException e) {

            throw new DlqPublishException(
                    "Failed to publish record to DLQ",
                    e
            );
        }
    }


    private void addHeader(
            ProducerRecord<byte[], byte[]> record,
            String key,
            String value
    ) {

        if (value == null) {
            return;
        }

        record.headers().add(
                key,
                value.getBytes(
                        StandardCharsets.UTF_8
                )
        );
    }
}