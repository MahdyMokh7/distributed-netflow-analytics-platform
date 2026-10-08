package com.nimbo.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.nimbo.application.ConsumerInstanceFactory;
import com.nimbo.application.ConsumerManager;
import com.nimbo.application.RegisteredSchemaLoader;
import com.nimbo.common.validation.NetflowValidationService;
import com.nimbo.config.HdfsProperties;
import com.nimbo.config.KafkaProperties;
import com.nimbo.config.ParquetProperties;
import com.nimbo.config.SchemaRegistryProperties;
import com.nimbo.ingestion.DlqService;
import com.nimbo.ingestion.KafkaConsumerFactory;
import com.nimbo.ingestion.KafkaOffsetCommitter;
import com.nimbo.metrics.ParquetWriterMetrics;
import com.nimbo.storage.HourWindowPolicy;
import com.nimbo.storage.batching.BatchPolicy;
import com.nimbo.storage.hdfs.HdfsFileSystemFactory;
import com.nimbo.storage.naming.FileNameGenerator;
import com.nimbo.support.KafkaContainerExtension;
import com.nimbo.support.NetFlowTestData;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

class ConsumerManagerIT {

  @RegisterExtension static final KafkaContainerExtension KAFKA = new KafkaContainerExtension();

  @TempDir Path temporaryDirectory;

  @Test
  void givenTwoConfiguredConsumers_whenManagerRuns_thenBothLifecyclesAreManaged() throws Exception {
    // Given: real Kafka and isolated local storage configured for two consumers
    String topic = "manager-netflow-topic";
    String dlqTopic = "manager-netflow-dlq";
    KAFKA.createTopics(topic, dlqTopic);
    KafkaProperties kafkaProperties = kafkaProperties(topic, dlqTopic);
    HdfsProperties hdfsProperties = hdfsProperties();
    ParquetProperties parquetProperties = parquetProperties();
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    ParquetWriterMetrics metrics = new ParquetWriterMetrics(registry);
    SchemaRegistryProperties schemaRegistryProperties = new SchemaRegistryProperties();
    schemaRegistryProperties.setUrl(KAFKA.schemaRegistryUrl());
    schemaRegistryProperties.setSubject(topic + "-value");

    try (var dlqProducer = KAFKA.producer();
        var schemaRegistryClient = new CachedSchemaRegistryClient(KAFKA.schemaRegistryUrl(), 100)) {
      schemaRegistryClient.register(
          schemaRegistryProperties.getSubject(),
          new AvroSchema(new NetFlowTestData().sourceSchema()));

      ConsumerManager manager =
          new ConsumerManager(
              kafkaProperties,
              new RegisteredSchemaLoader(schemaRegistryProperties, schemaRegistryClient),
              new ConsumerInstanceFactory(
                  kafkaProperties,
                  schemaRegistryProperties,
                  new NetflowValidationService(),
                  new DlqService(dlqProducer, kafkaProperties),
                  new BatchPolicy(5, 1024 * 1024, Duration.ofMinutes(2)),
                  new HourWindowPolicy(1, true, Clock.systemUTC()),
                  parquetProperties,
                  new FileNameGenerator(),
                  hdfsProperties,
                  new KafkaConsumerFactory(kafkaProperties),
                  new HdfsFileSystemFactory(),
                  new KafkaOffsetCommitter(),
                  metrics));

      // When: the manager starts and then shuts down every configured instance
      manager.start();
      await()
          .atMost(Duration.ofSeconds(10))
          .pollInterval(Duration.ofMillis(100))
          .until(() -> running(registry, 1) == 1.0 && running(registry, 2) == 1.0);
      manager.shutdown();

      // Then: both registered consumer gauges report a completed lifecycle
      await()
          .atMost(Duration.ofSeconds(5))
          .pollInterval(Duration.ofMillis(100))
          .untilAsserted(
              () -> {
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
    properties.setLateTopic(dlqTopic + "-late");
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
    return registry
        .get("ingester_consumer_running")
        .tag("instance", String.valueOf(instanceId))
        .gauge()
        .value();
  }
}
