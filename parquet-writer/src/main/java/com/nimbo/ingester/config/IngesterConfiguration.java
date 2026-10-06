package com.nimbo.ingester.config;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({KafkaProperties.class, SchemaRegistryProperties.class})
public class IngesterConfiguration {

  @Bean(destroyMethod = "close")
  public KafkaProducer<byte[], byte[]> kafkaProducer(KafkaProperties properties) {

    Map<String, Object> configs = new HashMap<>();

    configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
    configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    configs.put(ProducerConfig.ACKS_CONFIG, "all");
    configs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

    return new KafkaProducer<>(configs);
  }

  @Bean(destroyMethod = "close")
  public SchemaRegistryClient schemaRegistryClient(SchemaRegistryProperties properties) {
    return new CachedSchemaRegistryClient(properties.getUrl(), 1000);
  }
}
