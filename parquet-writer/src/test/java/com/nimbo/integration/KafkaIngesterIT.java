package com.nimbo.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.nimbo.ingester.config.KafkaProperties;
import com.nimbo.ingester.consumer.KafkaConsumerFactory;
import com.nimbo.ingester.consumer.KafkaOffsetCommitter;
import com.nimbo.ingester.service.DlqService;
import com.nimbo.storage.model.TopicPartitionKey;
import com.nimbo.support.KafkaContainerExtension;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class KafkaIngesterIT {

  @RegisterExtension static final KafkaContainerExtension KAFKA = new KafkaContainerExtension();

  @Test
  void givenProductionConsumerFactory_whenRecordPublished_thenConsumerReceivesExactPayload()
      throws Exception {
    // Given: a real topic and consumer built from production Kafka properties
    String topic = "factory-netflow-topic";
    String group = "factory-netflow-group";
    KAFKA.createTopics(topic);
    KafkaProperties properties = kafkaProperties(topic, "factory-dlq-topic", group);
    try (var producer = KAFKA.producer();
        var consumer = new KafkaConsumerFactory(properties).createConsumer(1)) {
      consumer.subscribe(Collections.singletonList(topic));
      byte[] payload = "deterministic-netflow".getBytes(StandardCharsets.UTF_8);
      producer.send(new ProducerRecord<>(topic, new byte[] {7}, payload)).get();

      // When: the production consumer polls the real broker
      AtomicReference<ConsumerRecord<byte[], byte[]>> received = new AtomicReference<>();
      await()
          .atMost(Duration.ofSeconds(10))
          .pollInterval(Duration.ofMillis(100))
          .until(
              () -> {
                var records = consumer.poll(Duration.ofMillis(100));
                if (!records.isEmpty()) {
                  received.set(records.iterator().next());
                }
                return received.get() != null;
              });

      // Then: topic, key, and payload survive the production consumer configuration
      assertThat(received.get().topic()).isEqualTo(topic);
      assertThat(received.get().key()).containsExactly(7);
      assertThat(received.get().value()).containsExactly(payload);
    }
  }

  @Test
  void givenNextOffset_whenCommitted_thenOffsetIsDurable() {
    // Given: one real Kafka partition and a deterministic next offset
    String topic = "offset-commit-topic";
    String group = "offset-commit-group";
    KAFKA.createTopics(topic);
    try (var consumer = KAFKA.consumer(group)) {
      consumer.assign(java.util.List.of(new TopicPartition(topic, 0)));
      Map<TopicPartitionKey, Long> offsets = Map.of(new TopicPartitionKey(topic, 0), 43L);

      // When: the production committer synchronously records the offsets
      new KafkaOffsetCommitter().commit(consumer, offsets);

      // Then: a separate real consumer observes the exact committed next offset
      assertThat(KAFKA.committedOffset(group, topic, 0)).isEqualTo(43L);
    }
  }

  @Test
  void givenInvalidOriginalRecord_whenPublishedToDlq_thenDiagnosticHeadersArePreserved()
      throws Exception {
    // Given: a real DLQ and one deterministic invalid source record
    String sourceTopic = "dlq-source-topic";
    String dlqTopic = "dlq-target-topic";
    KAFKA.createTopics(sourceTopic, dlqTopic);
    KafkaProperties properties = kafkaProperties(sourceTopic, dlqTopic, "dlq-writer-group");
    try (var producer = KAFKA.producer();
        var reader = KAFKA.consumer("dlq-reader-group")) {
      DlqService service = new DlqService(producer, properties);
      reader.subscribe(Collections.singletonList(dlqTopic));
      ConsumerRecord<byte[], byte[]> original =
          new ConsumerRecord<>(sourceTopic, 0, 17L, new byte[] {9}, new byte[] {1, 2, 3});

      // When: the production DLQ service publishes the validation failure
      service.publish(original, "VALIDATION_ERROR", "IPV4_SRC_ADDR invalid IPv4: 999.1.1.1");
      AtomicReference<ConsumerRecord<byte[], byte[]>> received = new AtomicReference<>();
      await()
          .atMost(Duration.ofSeconds(10))
          .pollInterval(Duration.ofMillis(100))
          .until(
              () -> {
                var records = reader.poll(Duration.ofMillis(100));
                if (!records.isEmpty()) {
                  received.set(records.iterator().next());
                }
                return received.get() != null;
              });

      // Then: payload identity and every diagnostic header are available to operators
      assertThat(received.get().key()).containsExactly(9);
      assertThat(received.get().value()).containsExactly(1, 2, 3);
      assertThat(header(received.get(), "original-topic")).isEqualTo(sourceTopic);
      assertThat(header(received.get(), "original-partition")).isEqualTo("0");
      assertThat(header(received.get(), "original-offset")).isEqualTo("17");
      assertThat(header(received.get(), "error-type")).isEqualTo("VALIDATION_ERROR");
      assertThat(header(received.get(), "error-message"))
          .isEqualTo("IPV4_SRC_ADDR invalid IPv4: 999.1.1.1");
    }
  }

  private KafkaProperties kafkaProperties(String topic, String dlqTopic, String group) {
    KafkaProperties properties = new KafkaProperties();
    properties.setBootstrapServers(KAFKA.bootstrapServers());
    properties.setTopic(topic);
    properties.setDlqTopic(dlqTopic);
    properties.setConsumerGroup(group);
    properties.setPollTimeoutMs(100);
    properties.setMaxPollRecords(500);
    properties.setSessionTimeoutMs(10_000);
    properties.setHeartbeatIntervalMs(3_000);
    properties.setMaxPollIntervalMs(60_000);
    properties.setEnableAutoCommit(false);
    properties.setAutoOffsetReset("earliest");
    return properties;
  }

  private String header(ConsumerRecord<byte[], byte[]> record, String name) {
    var header = Objects.requireNonNull(record.headers().lastHeader(name));
    return new String(header.value(), StandardCharsets.UTF_8);
  }
}
