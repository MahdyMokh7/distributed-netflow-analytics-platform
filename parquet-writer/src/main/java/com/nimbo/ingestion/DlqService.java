package com.nimbo.ingestion;

import com.nimbo.common.kafka.KafkaDlqPublisher;
import com.nimbo.config.KafkaProperties;
import com.nimbo.ingestion.DlqReason;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.stereotype.Service;

@Service
public class DlqService {

  private static final String LATE_RECORD_ERROR_MESSAGE = "Kafka timestamp is outside the allowed hour window";

  private final KafkaProducer<byte[], byte[]> kafkaProducer;
  private final KafkaProperties kafkaProperties;

  public DlqService(KafkaProducer<byte[], byte[]> kafkaProducer, KafkaProperties kafkaProperties) {

    this.kafkaProducer = kafkaProducer;
    this.kafkaProperties = kafkaProperties;
  }

  public void publish(ConsumerRecord<byte[], byte[]> originalRecord, DlqReason reason, String errorMessage) {

    publishTo(kafkaProperties.getDlqTopic(), originalRecord, reason, errorMessage);
  }

  public void publishLate(ConsumerRecord<byte[], byte[]> originalRecord) {

    publishTo(kafkaProperties.getLateTopic(), originalRecord, DlqReason.LATE_RECORD, LATE_RECORD_ERROR_MESSAGE);
  }

  private void publishTo(
      String targetTopic,
      ConsumerRecord<byte[], byte[]> originalRecord,
      DlqReason reason,
      String errorMessage) {

    ProducerRecord<byte[], byte[]> targetRecord = createRecord(targetTopic, originalRecord, reason, errorMessage);

    KafkaDlqPublisher.publish(kafkaProducer, targetRecord, kafkaProperties.getDlqPublishTimeoutMs());
  }

  private ProducerRecord<byte[], byte[]> createRecord(
      String targetTopic,
      ConsumerRecord<byte[], byte[]> originalRecord,
      DlqReason reason,
      String errorMessage) {

    Headers headers = new RecordHeaders(originalRecord.headers());
    addHeader(headers, "original-topic", originalRecord.topic());
    addHeader(headers, "original-partition", String.valueOf(originalRecord.partition()));
    addHeader(headers, "original-offset", String.valueOf(originalRecord.offset()));
    addHeader(headers, "original-timestamp", String.valueOf(originalRecord.timestamp()));
    addHeader(headers, "error-type", reason.name());
    addHeader(headers, "error-message", errorMessage);

    Long timestamp = originalRecord.timestamp() >= 0 ? originalRecord.timestamp() : null;

    return new ProducerRecord<>(targetTopic, null, timestamp, originalRecord.key(), originalRecord.value(), headers);
  }

  private void addHeader(Headers headers, String name, String value) {
    if (value != null) {
      headers.add(name, value.getBytes(StandardCharsets.UTF_8));
    }
  }
}
