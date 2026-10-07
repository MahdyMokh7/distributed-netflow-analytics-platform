package com.nimbo.starrockswriter.ingestion;

import com.nimbo.common.kafka.KafkaDlqPublisher.PublishException;
import com.nimbo.starrockswriter.config.KafkaProperties;
import com.nimbo.starrockswriter.metrics.StarRocksWriterMetrics;
import com.nimbo.starrockswriter.processing.BatchProcessor;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;

@Slf4j
@RequiredArgsConstructor
public class ConsumerWorker implements Runnable {

  private static final int SCHEMA_NOT_FOUND_ERROR_CODE = 40403;
  private static final String REGISTRY_LOOKUP_MESSAGE = "error retrieving avro";
  private static final long REGISTRY_BACKOFF_BASE_MS = 1_000L;
  private static final long REGISTRY_BACKOFF_MAX_MS = 30_000L;
  private static final int REGISTRY_BACKOFF_MAX_SHIFT = 5;

  private final int workerNumber;
  private final KafkaConsumer<byte[], GenericRecord> consumer;
  private final KafkaProperties kafkaProperties;
  private final BatchProcessor batchProcessor;
  private final StarRocksWriterMetrics metrics;
  private final DlqPublisher dlqPublisher;
  private volatile boolean running = true;
  private final CountDownLatch stopSignal = new CountDownLatch(1);

  @Setter(AccessLevel.PACKAGE)
  private BackoffSleeper sleeper = millis -> stopSignal.await(millis, TimeUnit.MILLISECONDS);

  private int registryFailureCount;

  @Override
  public void run() {
    try {
      consumer.subscribe(List.of(kafkaProperties.getTopic()));
      metrics.workerStarted(workerNumber);
      log.info("StarRocks consumer worker {} started", workerNumber);

      while (running) {
        processIteration();
      }
    } catch (WakeupException exception) {
      if (running) {
        metrics.workerException(workerNumber, "unexpected_wakeup");
        log.error("Worker {} received unexpected wakeup", workerNumber, exception);
      }
    } catch (RuntimeException exception) {
      metrics.workerException(workerNumber, "unexpected_exception");
      log.error("Worker {} stopped due to unexpected error", workerNumber, exception);
    } finally {
      closeConsumer();
    }
  }

  void processIteration() {
    batchProcessor.drainCompletedLoads(consumer);

    if (batchProcessor.hasTooManyInFlightLoads()) {
      pauseKafka();
      consumerPoll(Duration.ofMillis(100), "backpressure");
      return;
    }
    if (batchProcessor.isKafkaPaused()) {
      resumeKafka();
    }

    ConsumerRecords<byte[], GenericRecord> records = consumerPoll(
        Duration.ofMillis(kafkaProperties.getPollTimeoutMs()), "normal");
    metrics.polled(workerNumber, records.count());
    batchProcessor.processRecords(records);
    batchProcessor.flushIfNeeded();
  }

  private ConsumerRecords<byte[], GenericRecord> consumerPoll(Duration timeout, String type) {
    Timer.Sample sample = metrics.startPollTimer();
    try {
      ConsumerRecords<byte[], GenericRecord> records = consumer.poll(timeout);
      metrics.pollCompleted(workerNumber, type, sample);
      registryFailureCount = 0;
      return records;
    } catch (RecordDeserializationException exception) {
      metrics.pollCompleted(workerNumber, type, sample);
      handleDeserializationFailure(exception);
      return ConsumerRecords.empty();
    } catch (RuntimeException exception) {
      metrics.pollCompleted(workerNumber, type, sample);
      throw exception;
    }
  }

  private void handleDeserializationFailure(RecordDeserializationException exception) {
    if (isRegistryFailure(exception)) {
      retrySameOffsetAfterRegistryFailure(exception);
      return;
    }

    publishToDlqAndSkip(exception);
  }

  private void retrySameOffsetAfterRegistryFailure(RecordDeserializationException exception) {
    TopicPartition partition = exception.topicPartition();
    registryFailureCount++;
    metrics.workerException(workerNumber, "schema_registry_failure");
    log.warn(
        "Schema Registry/infrastructure failure. NOT publishing to DLQ and NOT advancing the "
            + "source offset; retrying the same offset after backoff: topic={}, partition={}, "
            + "offset={}, attempt={}",
        partition.topic(),
        partition.partition(),
        exception.offset(),
        registryFailureCount,
        exception);

    consumer.seek(partition, exception.offset());
    sleepBeforeRegistryRetry();
  }

  private void sleepBeforeRegistryRetry() {
    int shift = Math.min(registryFailureCount - 1, REGISTRY_BACKOFF_MAX_SHIFT);
    long delayMs = Math.min(REGISTRY_BACKOFF_MAX_MS, REGISTRY_BACKOFF_BASE_MS << shift);
    try {
      sleeper.sleep(delayMs);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  private void publishToDlqAndSkip(RecordDeserializationException exception) {
    TopicPartition partition = exception.topicPartition();
    long nextOffset = exception.offset() + 1;

    try {
      dlqPublisher.publish(exception);
    } catch (PublishException publishException) {
      metrics.workerException(workerNumber, "dlq_publish_error");
      log.error(
          "DLQ publish failed; source offset will NOT be advanced: topic={}, partition={}, offset={}, "
              + "dlqTopic={}",
          partition.topic(),
          partition.partition(),
          exception.offset(),
          kafkaProperties.getDlqTopic(),
          publishException);
      throw publishException;
    }

    metrics.recordProcessed(workerNumber, "deserialization_dlq");
    log.error(
        "Deserialization failure moved to DLQ; advancing source record only after DLQ acknowledgement: "
            + "topic={}, partition={}, offset={}, dlqTopic={}",
        partition.topic(),
        partition.partition(),
        exception.offset(),
        kafkaProperties.getDlqTopic(),
        exception);

    batchProcessor.skipDeserializationFailure(partition, nextOffset);
    consumer.seek(partition, nextOffset);
  }

  private boolean isRegistryFailure(Throwable failure) {
    RestClientException registryError = findCause(failure, RestClientException.class);
    if (registryError != null) {
      // A missing schema ID is a record problem; a healthy Registry answered 40403.
      return !(registryError.getStatus() == 404
          && registryError.getErrorCode() == SCHEMA_NOT_FOUND_ERROR_CODE);
    }

    if (containsCause(failure, AuthenticationException.class)
        || containsCause(failure, AuthorizationException.class)
        || containsCause(failure, TimeoutException.class)
        || containsCause(failure, DisconnectException.class)
        || containsCause(failure, ThrottlingQuotaExceededException.class)
        || containsCause(failure, InterruptedIOException.class)
        || containsCause(failure, SocketTimeoutException.class)
        || containsCause(failure, ConnectException.class)
        || containsCause(failure, UnknownHostException.class)
        || containsCause(failure, NoRouteToHostException.class)
        || containsCause(failure, SSLException.class)) {
      return true;
    }

    return containsMessage(failure, REGISTRY_LOOKUP_MESSAGE)
        && containsCause(failure, IOException.class);
  }

  private boolean containsMessage(Throwable failure, String fragment) {
    String expected = fragment.toLowerCase(Locale.ROOT);
    for (Throwable current = failure; current != null; current = current.getCause()) {
      String message = current.getMessage();
      if (message != null && message.toLowerCase(Locale.ROOT).contains(expected)) {
        return true;
      }
    }
    return false;
  }

  private <T extends Throwable> boolean containsCause(Throwable failure, Class<T> type) {
    return findCause(failure, type) != null;
  }

  private <T extends Throwable> T findCause(Throwable failure, Class<T> type) {
    for (Throwable current = failure; current != null; current = current.getCause()) {
      if (type.isInstance(current)) {
        return type.cast(current);
      }
    }
    return null;
  }

  private void pauseKafka() {
    if (consumer.assignment().isEmpty() || batchProcessor.isKafkaPaused()) {
      return;
    }
    consumer.pause(consumer.assignment());
    batchProcessor.markKafkaPaused();
    metrics.backpressure(workerNumber, true);
    log.debug("Worker {} paused Kafka", workerNumber);
  }

  private void resumeKafka() {
    consumer.resume(consumer.assignment());
    batchProcessor.markKafkaResumed();
    metrics.backpressure(workerNumber, false);
    log.debug("Worker {} resumed Kafka", workerNumber);
  }

  private void closeConsumer() {
    try {
      batchProcessor.shutdown(consumer);
    } catch (KafkaException exception) {
      metrics.workerException(workerNumber, "shutdown_exception");
      log.error(
          "Kafka failure during batch processor shutdown for worker {}, topic={}, consumerGroup={}",
          workerNumber,
          kafkaProperties.getTopic(),
          kafkaProperties.getConsumerGroup(),
          exception);
      throw exception;
    } finally {
      consumer.close();
      metrics.unbindKafkaConsumer(workerNumber);
      metrics.workerStopped(workerNumber);
      log.info("StarRocks consumer worker {} stopped", workerNumber);
    }
  }

  public void stopWorker() {
    running = false;
    stopSignal.countDown();
    consumer.wakeup();
  }

  @FunctionalInterface
  interface BackoffSleeper {

    void sleep(long millis) throws InterruptedException;
  }
}
