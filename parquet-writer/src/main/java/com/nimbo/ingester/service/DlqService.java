package com.nimbo.ingester.service;

import com.nimbo.common.kafka.KafkaDlqPublisher;
import com.nimbo.ingester.config.KafkaProperties;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.stereotype.Service;

@Service
public class DlqService {

  private final KafkaProducer<byte[], byte[]> kafkaProducer;
  private final KafkaProperties kafkaProperties;

  public DlqService(
      KafkaProducer<byte[], byte[]> kafkaProducer,
      KafkaProperties kafkaProperties) {
    this.kafkaProducer = kafkaProducer;
    this.kafkaProperties = kafkaProperties;
  }

  public void publish(
      ConsumerRecord<byte[], byte[]> originalRecord,
      String errorType,
      String errorMessage) {
    ProducerRecord<byte[], byte[]> dlqRecord = createDlqRecord(
        originalRecord, errorType, errorMessage);

    KafkaDlqPublisher.publish(
        kafkaProducer,
        dlqRecord,
        kafkaProperties.getDlqPublishTimeoutMs());
  }

  private ProducerRecord<byte[], byte[]> createDlqRecord(
      ConsumerRecord<byte[], byte[]> originalRecord,
      String errorType,
      String errorMessage) {
    Headers headers = new RecordHeaders(originalRecord.headers());
    addHeader(headers, "original-topic", originalRecord.topic());
    addHeader(headers, "original-partition", String.valueOf(originalRecord.partition()));
    addHeader(headers, "original-offset", String.valueOf(originalRecord.offset()));
    addHeader(headers, "original-timestamp", String.valueOf(originalRecord.timestamp()));
    addHeader(headers, "error-type", errorType);
    addHeader(headers, "error-message", errorMessage);

    Long timestamp = originalRecord.timestamp() >= 0 ? originalRecord.timestamp() : null;
    return new ProducerRecord<>(
        kafkaProperties.getDlqTopic(),
        null,
        timestamp,
        originalRecord.key(),
        originalRecord.value(),
        headers);
  }

  private void addHeader(Headers headers, String name, String value) {
    if (value != null) {
      headers.add(name, value.getBytes(StandardCharsets.UTF_8));
    }
  }
}
