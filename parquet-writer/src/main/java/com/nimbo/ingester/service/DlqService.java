package com.nimbo.ingester.service;

import com.nimbo.ingester.config.KafkaProperties;
import com.nimbo.ingester.exception.DlqPublishException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;

@Service
public class DlqService {

  private final KafkaProducer<byte[], byte[]> kafkaProducer;
  private final KafkaProperties kafkaProperties;

  public DlqService(KafkaProducer<byte[], byte[]> kafkaProducer, KafkaProperties kafkaProperties) {
    this.kafkaProducer = kafkaProducer;
    this.kafkaProperties = kafkaProperties;
  }

  public void publish(
      ConsumerRecord<byte[], byte[]> originalRecord, String errorType, String errorMessage) {

    ProducerRecord<byte[], byte[]> dlqRecord =
        new ProducerRecord<>(
            kafkaProperties.getDlqTopic(), originalRecord.key(), originalRecord.value());

    addHeader(dlqRecord, "original-topic", originalRecord.topic());
    addHeader(dlqRecord, "original-partition", String.valueOf(originalRecord.partition()));
    addHeader(dlqRecord, "original-offset", String.valueOf(originalRecord.offset()));
    addHeader(dlqRecord, "error-type", errorType);
    addHeader(dlqRecord, "error-message", errorMessage);

    try {
      kafkaProducer.send(dlqRecord).get();

    } catch (InterruptedException e) {

      Thread.currentThread().interrupt();

      throw new DlqPublishException("Interrupted while publishing record to DLQ", e);

    } catch (ExecutionException e) {

      throw new DlqPublishException("Failed to publish record to DLQ", e);
    }
  }

  private void addHeader(ProducerRecord<byte[], byte[]> dlqRecord, String key, String value) {

    if (value == null) {
      return;
    }

    dlqRecord.headers().add(key, value.getBytes(StandardCharsets.UTF_8));
  }
}
