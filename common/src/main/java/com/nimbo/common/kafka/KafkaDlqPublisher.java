package com.nimbo.common.kafka;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;

public final class KafkaDlqPublisher {

  private KafkaDlqPublisher() {
  }

  public static RecordMetadata publish(Producer<byte[], byte[]> producer,
      ProducerRecord<byte[], byte[]> producerRecord, long timeoutMs) {
    try {
      return producer.send(producerRecord).get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new PublishException("Interrupted while publishing record to DLQ", exception);
    } catch (ExecutionException | TimeoutException | KafkaException exception) {
      throw new PublishException("Failed to publish record to DLQ", exception);
    }
  }

  public static final class PublishException extends RuntimeException {

    public PublishException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
