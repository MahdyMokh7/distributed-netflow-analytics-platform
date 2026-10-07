package com.nimbo.starrockswriter.ingestion;

import static org.apache.kafka.common.errors.RecordDeserializationException.DeserializationExceptionOrigin.VALUE;
import static org.assertj.core.api.Assertions.assertThat;

import com.nimbo.starrockswriter.config.KafkaProperties;
import com.nimbo.starrockswriter.metrics.StarRocksWriterMetrics;
import com.nimbo.starrockswriter.processing.BatchProcessor;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConsumerWorkerTest {

  private static final int WORKER_NUMBER = 1;
  private static final TopicPartition PARTITION = new TopicPartition("multi-version-netflow", 3);
  private static final long FAILED_OFFSET = 42L;

  private ScriptedConsumer consumer;
  private RecordingBatchProcessor batchProcessor;
  private RecordingDlqPublisher dlqPublisher;
  private List<Long> backoffDelays;
  private ConsumerWorker worker;

  @BeforeEach
  void setUp() {
    KafkaProperties properties = new KafkaProperties();
    properties.setTopic(PARTITION.topic());
    properties.setDlqTopic("netflow-dlq");
    properties.setPollTimeoutMs(100);

    StarRocksWriterMetrics metrics = new StarRocksWriterMetrics(new SimpleMeterRegistry());
    metrics.registerWorker(WORKER_NUMBER);

    consumer = new ScriptedConsumer();
    batchProcessor = new RecordingBatchProcessor();
    dlqPublisher = new RecordingDlqPublisher(properties);
    backoffDelays = new ArrayList<>();

    worker =
        new ConsumerWorker(
            WORKER_NUMBER, consumer, properties, batchProcessor, metrics, dlqPublisher);
    worker.setSleeper(backoffDelays::add);
  }

  @AfterEach
  void tearDown() {
    Thread.interrupted();
    consumer.close(Duration.ZERO);
  }

  @Test
  void givenRegistryOutage_whenPolling_thenSameOffsetIsRetriedAndWorkerRecovers() {
    // Given: the Registry is down for two polls and then returns
    consumer.fail(registryFailure(503, 50301)).fail(registryFailure(503, 50301)).succeed();

    // When: the worker runs three iterations
    worker.processIteration();
    worker.processIteration();
    worker.processIteration();

    // Then: nothing reached the DLQ and the source offset never advanced
    assertThat(dlqPublisher.published).isEmpty();
    assertThat(batchProcessor.skipped).isEmpty();
    assertThat(consumer.seeks).containsExactly(seek(FAILED_OFFSET), seek(FAILED_OFFSET));

    // And: the worker stayed alive, backed off, and polled again until the Registry returned
    assertThat(consumer.pollCount).isEqualTo(3);
    assertThat(backoffDelays).containsExactly(1_000L, 2_000L);
    assertThat(batchProcessor.processedBatches).isEqualTo(3);
  }

  @Test
  void givenLongRegistryOutage_whenPolling_thenBackoffIsExponentialAndBounded() {
    // Given: eight consecutive Registry failures
    for (int i = 0; i < 8; i++) {
      consumer.fail(registryFailure(503, 50301));
    }

    // When: the worker keeps retrying
    for (int i = 0; i < 8; i++) {
      worker.processIteration();
    }

    // Then: delays double up to the 30 second cap and the offset is never advanced
    assertThat(backoffDelays)
        .containsExactly(1_000L, 2_000L, 4_000L, 8_000L, 16_000L, 30_000L, 30_000L, 30_000L);
    assertThat(consumer.seeks).allMatch(seek -> seek.equals(seek(FAILED_OFFSET)));
    assertThat(dlqPublisher.published).isEmpty();
  }

  @Test
  void givenSuccessfulPoll_whenRegistryFailsAgain_thenBackoffStartsOver() {
    // Given: failures, a successful poll, then a new failure
    consumer.fail(registryFailure(503, 50301)).fail(registryFailure(503, 50301)).succeed()
        .fail(registryFailure(503, 50301));

    // When: the worker runs four iterations
    for (int i = 0; i < 4; i++) {
      worker.processIteration();
    }

    // Then: the second outage restarts from the base delay
    assertThat(backoffDelays).containsExactly(1_000L, 2_000L, 1_000L);
  }

  @Test
  void givenSchemaIdNotFound_whenPolling_thenRecordGoesToDlqAndOffsetAdvances() {
    // Given: a healthy Registry answered 404/40403 for the record's schema id
    consumer.fail(registryFailure(404, 40403));

    // When: the worker runs one iteration
    worker.processIteration();

    // Then: the poison record is published to the DLQ and skipped, without any backoff
    assertThat(dlqPublisher.published).hasSize(1);
    assertThat(batchProcessor.skipped).containsExactly(seek(FAILED_OFFSET + 1));
    assertThat(consumer.seeks).containsExactly(seek(FAILED_OFFSET + 1));
    assertThat(backoffDelays).isEmpty();
  }

  @Test
  void givenInterruptedBackoff_whenRegistryIsDown_thenInterruptFlagIsRestored() {
    // Given: the backoff wait is interrupted (for example by executor shutdownNow)
    consumer.fail(registryFailure(503, 50301));
    worker.setSleeper(
        millis -> {
          throw new InterruptedException("shutdown");
        });

    // When: the worker handles the Registry failure
    worker.processIteration();

    // Then: the offset was still not advanced and the interrupt is visible to the run loop
    assertThat(dlqPublisher.published).isEmpty();
    assertThat(consumer.seeks).containsExactly(seek(FAILED_OFFSET));
    assertThat(Thread.currentThread().isInterrupted()).isTrue();
  }

  @Test
  void givenPartitionsRevoked_whenRebalancing_thenPendingLoadsAreCommittedFirst() {
    worker.rebalanceListener().onPartitionsRevoked(List.of(PARTITION));

    assertThat(batchProcessor.rebalancePreparations).isEqualTo(1);
    assertThat(batchProcessor.rebalanceConsumer).isSameAs(consumer);
  }

  private static RecordDeserializationException registryFailure(int status, int errorCode) {
    return new RecordDeserializationException(
        VALUE,
        PARTITION,
        FAILED_OFFSET,
        123_456_789L,
        TimestampType.CREATE_TIME,
        ByteBuffer.wrap(new byte[] {1}),
        ByteBuffer.wrap(new byte[] {0, 0, 0, 0, 3}),
        new RecordHeaders(),
        "Error deserializing VALUE",
        new SerializationException(
            "Error retrieving Avro schema for id 3",
            new RestClientException("Schema Registry error", status, errorCode)));
  }

  private static SeekCall seek(long offset) {
    return new SeekCall(PARTITION, offset);
  }

  private record SeekCall(TopicPartition partition, long offset) {
  }

  private static final class ScriptedConsumer extends KafkaConsumer<byte[], GenericRecord> {

    private final Queue<RuntimeException> script = new ArrayDeque<>();
    private final List<SeekCall> seeks = new ArrayList<>();
    private int pollCount;

    ScriptedConsumer() {
      super(
          Map.<String, Object>of(
              ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
              ConsumerConfig.GROUP_ID_CONFIG, "consumer-worker-test"),
          (topic, data) -> data,
          (topic, data) -> null);
    }

    ScriptedConsumer fail(RuntimeException exception) {
      script.add(exception);
      return this;
    }

    ScriptedConsumer succeed() {
      script.add(new NoFailure());
      return this;
    }

    @Override
    public ConsumerRecords<byte[], GenericRecord> poll(Duration timeout) {
      pollCount++;
      RuntimeException next = script.poll();
      if (next != null && !(next instanceof NoFailure)) {
        throw next;
      }
      return ConsumerRecords.empty();
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
      seeks.add(new SeekCall(partition, offset));
    }

    private static final class NoFailure extends RuntimeException {
    }
  }

  private static final class RecordingBatchProcessor extends BatchProcessor {

    private final List<SeekCall> skipped = new ArrayList<>();
    private int processedBatches;
    private int rebalancePreparations;
    private KafkaConsumer<byte[], GenericRecord> rebalanceConsumer;

    RecordingBatchProcessor() {
      super(WORKER_NUMBER, null, null, null, null, null, null, null, null);
    }

    @Override
    public void drainCompletedLoads(KafkaConsumer<byte[], GenericRecord> kafkaConsumer) {
    }

    @Override
    public boolean hasTooManyInFlightLoads() {
      return false;
    }

    @Override
    public boolean isKafkaPaused() {
      return false;
    }

    @Override
    public void processRecords(ConsumerRecords<byte[], GenericRecord> records) {
      processedBatches++;
    }

    @Override
    public void flushIfNeeded() {
    }

    @Override
    public void skipDeserializationFailure(TopicPartition partition, long nextOffset) {
      skipped.add(new SeekCall(partition, nextOffset));
    }

    @Override
    public void prepareForRebalance(KafkaConsumer<byte[], GenericRecord> kafkaConsumer) {
      rebalancePreparations++;
      rebalanceConsumer = kafkaConsumer;
    }
  }

  private static final class RecordingDlqPublisher extends DlqPublisher {

    private final List<RecordDeserializationException> published = new ArrayList<>();

    RecordingDlqPublisher(KafkaProperties properties) {
      super(null, properties);
    }

    @Override
    public void publish(RecordDeserializationException exception) {
      published.add(exception);
    }
  }
}
