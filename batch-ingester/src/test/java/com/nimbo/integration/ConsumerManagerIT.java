package com.nimbo.integration;

import com.nimbo.common.validation.NetflowValidationService;
import com.nimbo.ingester.config.KafkaProperties;
import com.nimbo.ingester.consumer.ConsumerManager;
import com.nimbo.ingester.consumer.KafkaConsumerFactory;
import com.nimbo.ingester.consumer.KafkaOffsetCommitter;
import com.nimbo.ingester.deserializer.AvroDeserializer;
import com.nimbo.ingester.service.DlqService;
import com.nimbo.metrics.BatchIngesterMetrics;
import com.nimbo.storage.config.HdfsProperties;
import com.nimbo.storage.config.ParquetProperties;
import com.nimbo.storage.service.batching.BatchPolicy;
import com.nimbo.storage.service.hdfs.HdfsFileSystemFactory;
import com.nimbo.storage.service.naming.FileNameGenerator;
import com.nimbo.support.KafkaContainerExtension;
import com.nimbo.support.NetFlowTestData;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class ConsumerManagerIT {

    @RegisterExtension
    static final KafkaContainerExtension KAFKA = new KafkaContainerExtension();

    @TempDir
    Path temporaryDirectory;

    @Test
    void givenTwoConfiguredConsumers_whenManagerRuns_thenBothLifecyclesAreManaged() {
        // Given: real Kafka and isolated local storage configured for two consumers
        String topic = "manager-netflow-topic";
        String dlqTopic = "manager-netflow-dlq";
        KAFKA.createTopics(topic, dlqTopic);
        KafkaProperties kafkaProperties = kafkaProperties(topic, dlqTopic);
        HdfsProperties hdfsProperties = hdfsProperties();
        ParquetProperties parquetProperties = parquetProperties();
        NetFlowTestData testData = new NetFlowTestData();
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        BatchIngesterMetrics metrics = new BatchIngesterMetrics(registry);
        try (var dlqProducer = KAFKA.producer()) {
            ConsumerManager manager = new ConsumerManager(
                    kafkaProperties,
                    new AvroDeserializer(testData.sourceSchema()),
                    new NetflowValidationService(),
                    new DlqService(dlqProducer, kafkaProperties),
                    new BatchPolicy(5, 1024 * 1024, Duration.ofMinutes(2)),
                    parquetProperties,
                    new FileNameGenerator(),
                    hdfsProperties,
                    new KafkaConsumerFactory(kafkaProperties),
                    new HdfsFileSystemFactory(),
                    new KafkaOffsetCommitter(),
                    testData.storageSchema(),
                    metrics);

            // When: the manager starts and then shuts down every configured instance
            manager.start();
            await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(100)).until(() ->
                    running(registry, 1) == 1.0 && running(registry, 2) == 1.0);
            manager.shutdown();

            // Then: both registered consumer gauges report a completed lifecycle
            await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(100)).untilAsserted(() -> {
                assertThat(running(registry, 1)).isZero();
                assertThat(running(registry, 2)).isZero();
            });
        } finally {
            registry.close();
        }
    }

    private KafkaProperties kafkaProperties(String topic, String dlqTopic) {
        KafkaProperties properties = new KafkaProperties();
        properties.setBootstrapServers(KAFKA.bootstrapServers());
        properties.setTopic(topic);
        properties.setDlqTopic(dlqTopic);
        properties.setConsumerGroup("manager-consumer-group");
        properties.setEnableAutoCommit(false);
        properties.setAutoOffsetReset("earliest");
        properties.setPollTimeoutMs(100);
        properties.setMaxPollRecords(500);
        properties.setSessionTimeoutMs(10_000);
        properties.setHeartbeatIntervalMs(3_000);
        properties.setMaxPollIntervalMs(60_000);
        properties.setConsumerInstances(2);
        return properties;
    }

    private HdfsProperties hdfsProperties() {
        HdfsProperties properties = new HdfsProperties();
        properties.setUri(temporaryDirectory.toUri().toString());
        properties.setBasePath(temporaryDirectory.resolve("netflow/manager").toString());
        return properties;
    }

    private ParquetProperties parquetProperties() {
        ParquetProperties properties = new ParquetProperties();
        properties.setCompressionCodec("SNAPPY");
        properties.setBlockSizeBytes(128 * 1024 * 1024);
        properties.setPageSizeBytes(1024 * 1024);
        properties.setEnableDictionary(true);
        return properties;
    }

    private double running(SimpleMeterRegistry registry, int instanceId) {
        return registry.get("ingester_consumer_running")
                .tag("instance", String.valueOf(instanceId))
                .gauge()
                .value();
    }
}
