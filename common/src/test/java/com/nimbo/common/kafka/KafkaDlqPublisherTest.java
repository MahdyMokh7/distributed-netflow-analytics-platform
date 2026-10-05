package com.nimbo.common.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.nimbo.common.kafka.KafkaDlqPublisher.PublishException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaDlqPublisherTest {

  private static final long TIMEOUT_MS = 1_500L;

  private final ProducerRecord<byte[], byte[]> dlqRecord =
      new ProducerRecord<>("netflow-dlq", new byte[] {1}, new byte[] {2});

  private Producer<byte[], byte[]> producer;
  private Future<RecordMetadata> acknowledgement;

  @SuppressWarnings("unchecked")
  @BeforeEach
  void setUp() {
    producer = mock(Producer.class);
    acknowledgement = mock(Future.class);
    when(producer.send(dlqRecord)).thenReturn(acknowledgement);
  }

  @AfterEach
  void clearInterruptFlag() {
    Thread.interrupted();
  }

  @Test
  void givenBrokerAcknowledges_whenPublished_thenMetadataIsReturnedUsingConfiguredTimeout()
      throws Exception {
    // Given: the broker acknowledges the DLQ record
    RecordMetadata metadata = mock(RecordMetadata.class);
    when(acknowledgement.get(TIMEOUT_MS, TimeUnit.MILLISECONDS)).thenReturn(metadata);

    // When: the record is published
    RecordMetadata actual = KafkaDlqPublisher.publish(producer, dlqRecord, TIMEOUT_MS);

    // Then: the acknowledgement metadata is returned and the configured timeout was used
    assertThat(actual).isSameAs(metadata);
    verify(acknowledgement).get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
  }

  @Test
  void givenAcknowledgementTimesOut_whenPublished_thenTimeoutIsWrapped() throws Exception {
    // Given: the broker does not acknowledge within the timeout
    when(acknowledgement.get(anyLong(), any())).thenThrow(new TimeoutException("slow broker"));

    // When / Then: callers receive the shared wrapper with the timeout as the cause
    assertThatThrownBy(() -> KafkaDlqPublisher.publish(producer, dlqRecord, TIMEOUT_MS))
        .isInstanceOf(PublishException.class)
        .hasMessage("Failed to publish record to DLQ")
        .hasCauseInstanceOf(TimeoutException.class);
  }

  @Test
  void givenBrokerRejectsRecord_whenPublished_thenExecutionExceptionIsWrapped()
      throws Exception {
    // Given: the producer future completes exceptionally
    when(acknowledgement.get(anyLong(), any()))
        .thenThrow(new ExecutionException(new IllegalStateException("rejected")));

    // When / Then: the failure is wrapped without losing the original exception
    assertThatThrownBy(() -> KafkaDlqPublisher.publish(producer, dlqRecord, TIMEOUT_MS))
        .isInstanceOf(PublishException.class)
        .hasMessage("Failed to publish record to DLQ")
        .hasCauseInstanceOf(ExecutionException.class);
  }

  @Test
  void givenProducerSendFails_whenPublished_thenKafkaExceptionIsWrapped() {
    // Given: send itself throws before a future exists
    when(producer.send(dlqRecord)).thenThrow(new KafkaException("producer closed"));

    // When / Then: the failure is wrapped as a publish failure
    assertThatThrownBy(() -> KafkaDlqPublisher.publish(producer, dlqRecord, TIMEOUT_MS))
        .isInstanceOf(PublishException.class)
        .hasMessage("Failed to publish record to DLQ")
        .hasCauseInstanceOf(KafkaException.class);
  }

  @Test
  void givenThreadIsInterrupted_whenPublished_thenFailureIsWrappedAndInterruptFlagRestored()
      throws Exception {
    // Given: waiting for the acknowledgement is interrupted
    when(acknowledgement.get(anyLong(), any())).thenThrow(new InterruptedException("shutdown"));

    // When / Then: the failure is wrapped with the interruption-specific message
    assertThatThrownBy(() -> KafkaDlqPublisher.publish(producer, dlqRecord, TIMEOUT_MS))
        .isInstanceOf(PublishException.class)
        .hasMessage("Interrupted while publishing record to DLQ")
        .hasCauseInstanceOf(InterruptedException.class);

    // And: the thread's interrupt status is restored for the caller's shutdown logic
    assertThat(Thread.currentThread().isInterrupted()).isTrue();
  }
}
