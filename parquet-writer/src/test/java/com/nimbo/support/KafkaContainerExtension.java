package com.nimbo.support;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public final class KafkaContainerExtension implements BeforeAllCallback {

  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(KafkaContainerExtension.class);
  private static final Object KAFKA_KEY = new Object();

  private SharedKafka kafka;

  @Override
  public void beforeAll(ExtensionContext context) {
    kafka =
        context
            .getRoot()
            .getStore(NAMESPACE)
            .getOrComputeIfAbsent(
                KAFKA_KEY, ignored -> SharedKafkaHolder.INSTANCE, SharedKafka.class);
  }

  public String bootstrapServers() {
    return kafka().container.getBootstrapServers();
  }

  public String schemaRegistryUrl() {
    GenericContainer<?> schemaRegistry = kafka().schemaRegistry;
    return "http://" + schemaRegistry.getHost() + ':' + schemaRegistry.getMappedPort(8081);
  }

  public void createTopics(String... topics) {
    Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    try (AdminClient admin = AdminClient.create(properties)) {

      Set<String> existingTopics = admin.listTopics().names().get();
      List<String> existingTopicsToDelete =
          Arrays.stream(topics).filter(existingTopics::contains).toList();
      if (!existingTopicsToDelete.isEmpty()) {

        admin.deleteTopics(existingTopicsToDelete).all().get();
        org.awaitility.Awaitility.await()
            .atMost(java.time.Duration.ofSeconds(30))
            .until(
                () -> {
                  var names =
                      admin.listTopics().names().get(5, java.util.concurrent.TimeUnit.SECONDS);
                  return existingTopicsToDelete.stream().noneMatch(names::contains);
                });
      }

      List<NewTopic> definitions =
          Arrays.stream(topics).map(topic -> new NewTopic(topic, 1, (short) 1)).toList();

      admin.createTopics(definitions).all().get();

    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while creating Kafka test topics", exception);

    } catch (ExecutionException | TimeoutException exception) {
      throw new IllegalStateException("Failed to create Kafka test topics", exception);
    }
  }

  public KafkaProducer<byte[], byte[]> producer() {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    properties.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    return new KafkaProducer<>(properties);
  }

  public KafkaConsumer<byte[], byte[]> consumer(String groupId) {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    properties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10_000);
    properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3_000);
    properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300_000);
    return new KafkaConsumer<>(properties);
  }

  public Long committedOffset(String groupId, String topic, int partition) {
    try (KafkaConsumer<byte[], byte[]> consumer = consumer(groupId)) {
      TopicPartition topicPartition = new TopicPartition(topic, partition);
      Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> committed =
          consumer.committed(Set.of(topicPartition), Duration.ofSeconds(5));
      var metadata = committed.get(topicPartition);
      return metadata == null ? null : metadata.offset();
    }
  }

  private SharedKafka kafka() {
    if (kafka == null) {
      kafka = SharedKafkaHolder.INSTANCE;
    }
    return kafka;
  }

  private static final class SharedKafkaHolder {
    private static final SharedKafka INSTANCE = new SharedKafka();
  }

  private static final class SharedKafka implements ExtensionContext.Store.CloseableResource {
    private final Network network = Network.newNetwork();
    private final KafkaContainer container =
        new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"))
            .withNetwork(network)
            .withNetworkAliases("kafka")
            .withListener("kafka:19092");
    private final GenericContainer<?> schemaRegistry =
        new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:7.8.0"))
            .withNetwork(network)
            .withExposedPorts(8081)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:19092")
            .waitingFor(Wait.forHttp("/subjects").forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(2));

    private SharedKafka() {
      container.start();
      schemaRegistry.start();
    }

    @Override
    public void close() {
      schemaRegistry.stop();
      container.stop();
      network.close();
    }
  }
}
