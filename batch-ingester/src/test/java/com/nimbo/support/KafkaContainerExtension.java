package com.nimbo.support;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public final class KafkaContainerExtension implements BeforeAllCallback {

    private static final ExtensionContext.Namespace NAMESPACE =
            ExtensionContext.Namespace.create(KafkaContainerExtension.class);
    private static final Object KAFKA_KEY = new Object();

    private SharedKafka kafka;

    @Override
    public void beforeAll(ExtensionContext context) {
        kafka = context.getRoot().getStore(NAMESPACE)
                .getOrComputeIfAbsent(KAFKA_KEY, ignored -> SharedKafkaHolder.INSTANCE, SharedKafka.class);
    }

    public String bootstrapServers() {
        return kafka().container.getBootstrapServers();
    }

    public void createTopics(String... topics) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        try (AdminClient admin = AdminClient.create(properties)) {
            List<NewTopic> definitions = java.util.Arrays.stream(topics)
                    .map(topic -> new NewTopic(topic, 1, (short) 1))
                    .toList();
            admin.createTopics(definitions).all().get();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create deterministic Kafka test topics", e);
        }
    }

    public KafkaProducer<byte[], byte[]> producer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(properties);
    }

    public KafkaConsumer<byte[], byte[]> consumer(String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10_000);
        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3_000);
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 60_000);
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
        private final KafkaContainer container =
                new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));

        private SharedKafka() {
            container.start();
        }

        @Override
        public void close() {
            container.stop();
        }
    }
}
