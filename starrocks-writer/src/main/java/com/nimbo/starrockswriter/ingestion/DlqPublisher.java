package com.nimbo.starrockswriter.ingestion;

import com.nimbo.common.kafka.KafkaDlqPublisher;
import com.nimbo.starrockswriter.config.KafkaProperties;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class DlqPublisher {

  private static final String ERROR_TYPE = "DESERIALIZATION_ERROR";

  private final KafkaProducer<byte[], byte[]> kafkaProducer;
  private final KafkaProperties kafkaProperties;

  public void publish(RecordDeserializationException exception) {
    ProducerRecord<byte[], byte[]> dlqRecord = createDlqRecord(exception);
    RecordMetadata metadata = KafkaDlqPublisher.publish(
        kafkaProducer,
        dlqRecord,
        kafkaProperties.getDlqPublishTimeoutMs());

    log.warn(
        "Published deserialization failure to DLQ: sourceTopic={}, sourcePartition={}, "
            + "sourceOffset={}, dlqTopic={}, dlqPartition={}, dlqOffset={}",
        exception.topicPartition().topic(),
        exception.topicPartition().partition(),
        exception.offset(),
        metadata.topic(),
        metadata.partition(),
        metadata.offset());
  }

  ProducerRecord<byte[], byte[]> createDlqRecord(RecordDeserializationException exception) {
    Headers headers = exception.headers() == null
        ? new RecordHeaders()
        : new RecordHeaders(exception.headers());

    addHeader(headers, "original-topic", exception.topicPartition().topic());
    addHeader(headers, "original-partition", String.valueOf(exception.topicPartition().partition()));
    addHeader(headers, "original-offset", String.valueOf(exception.offset()));
    addHeader(headers, "original-timestamp", String.valueOf(exception.timestamp()));
    addHeader(headers, "error-type", ERROR_TYPE);
    addHeader(headers, "error-class", exception.getClass().getName());
    addHeader(headers, "error-message", exception.getMessage());

    if (exception.getCause() != null) {
      addHeader(headers, "error-cause-class", exception.getCause().getClass().getName());
      addHeader(headers, "error-cause-message", exception.getCause().getMessage());
    }
    if (exception.origin() != null) {
      addHeader(headers, "deserialization-origin", exception.origin().name());
    }

    Long timestamp = exception.timestamp() >= 0 ? exception.timestamp() : null;
    return new ProducerRecord<>(
        kafkaProperties.getDlqTopic(),
        null,
        timestamp,
        toByteArray(exception.keyBuffer()),
        toByteArray(exception.valueBuffer()),
        headers);
  }

  private void addHeader(Headers headers, String name, String value) {
    if (value != null) {
      headers.add(name, value.getBytes(StandardCharsets.UTF_8));
    }
  }

  private byte[] toByteArray(ByteBuffer buffer) {
    if (buffer == null) {
      return null;
    }
    ByteBuffer duplicate = buffer.duplicate();
    byte[] bytes = new byte[duplicate.remaining()];
    duplicate.get(bytes);
    return bytes;
  }
}
