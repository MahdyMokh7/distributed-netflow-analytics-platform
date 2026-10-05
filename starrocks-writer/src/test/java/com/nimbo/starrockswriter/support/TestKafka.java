package com.nimbo.starrockswriter.support;

import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public final class TestKafka {

  private static final String IMAGE = "apache/kafka-native:3.8.0";
  private static final String TOPIC = "multi-version-netflow-integration-test";
  private static final String CONSUMER_GROUP = "starrockswriter-integration-test";
  private static final int PARTITIONS = 2;

  private final KafkaContainer container =
      new KafkaContainer(DockerImageName.parse(IMAGE)).withReuse(true);

  KafkaContainer container() {
    return container;
  }

  public String bootstrapServers() {
    return container.getBootstrapServers();
  }

  public String topic() {
    return TOPIC;
  }

  public String consumerGroup() {
    return CONSUMER_GROUP;
  }

  public long produce(byte[] payload, long timestamp, int partition) {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties)) {
      return producer
          .send(new ProducerRecord<>(TOPIC, partition, timestamp, null, payload))
          .get(30, TimeUnit.SECONDS)
          .offset();
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "Interrupted while producing an integration-test Kafka record", exception);
    } catch (ExecutionException | TimeoutException exception) {
      throw new IllegalStateException(
          "Could not produce an integration-test Kafka record", exception);
    }
  }

  public void awaitCommittedOffset(int partitionNumber, long expected) {
    TopicPartition partition = new TopicPartition(TOPIC, partitionNumber);
    await()
        .alias("Expected Kafka committed offset " + expected + " for partition " + partitionNumber)
        .pollInterval(Duration.ofMillis(500))
        .atMost(Duration.ofSeconds(60))
        .until(
            () -> {
              try (AdminClient admin = adminClient()) {
                Map<TopicPartition, OffsetAndMetadata> offsets =
                    admin
                        .listConsumerGroupOffsets(CONSUMER_GROUP)
                        .partitionsToOffsetAndMetadata()
                        .get(5, TimeUnit.SECONDS);
                return offsets.containsKey(partition)
                    && offsets.get(partition).offset() == expected;
              }
            });
  }

  public void awaitConsumerGroupMembers(int expected) {
    await()
        .alias("Expected " + expected + " starrocks consumers to join their Kafka group")
        .ignoreExceptions()
        .pollInterval(Duration.ofMillis(250))
        .atMost(Duration.ofSeconds(30))
        .until(
            () -> {
              try (AdminClient admin = adminClient()) {
                return admin
                    .describeConsumerGroups(List.of(CONSUMER_GROUP))
                    .describedGroups()
                    .get(CONSUMER_GROUP)
                    .get(5, TimeUnit.SECONDS)
                    .members()
                    .size()
                    == expected;
              }
            });
  }

  void reset() {
    try (AdminClient admin = adminClient()) {
      deleteConsumerGroup(admin);
      if (admin.listTopics().names().get(10, TimeUnit.SECONDS).contains(TOPIC)) {
        admin.deleteTopics(List.of(TOPIC)).all().get(30, TimeUnit.SECONDS);
        await()
            .alias("Kafka integration-test topic deletion")
            .pollInterval(Duration.ofMillis(250))
            .atMost(Duration.ofSeconds(30))
            .until(() -> !admin.listTopics().names().get(5, TimeUnit.SECONDS).contains(TOPIC));
      }
      admin
          .createTopics(List.of(new NewTopic(TOPIC, PARTITIONS, (short) 1)))
          .all()
          .get(30, TimeUnit.SECONDS);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "Interrupted while resetting the integration-test Kafka state", exception);
    } catch (ExecutionException | TimeoutException exception) {
      throw new IllegalStateException(
          "Could not reset the integration-test Kafka state", exception);
    }
  }

  private void deleteConsumerGroup(AdminClient admin)
      throws InterruptedException, ExecutionException, TimeoutException {
    try {
      admin.deleteConsumerGroups(List.of(CONSUMER_GROUP)).all().get(30, TimeUnit.SECONDS);
    } catch (ExecutionException exception) {
      if (!(exception.getCause() instanceof GroupIdNotFoundException)) {
        throw exception;
      }
    }
  }

  private AdminClient adminClient() {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    return AdminClient.create(properties);
  }
}
