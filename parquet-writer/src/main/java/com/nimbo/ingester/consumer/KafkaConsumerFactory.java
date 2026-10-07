package com.nimbo.ingester.consumer;

import com.nimbo.ingester.config.KafkaProperties;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.stereotype.Component;

@Component("nimboKafkaConsumerFactory")
public class KafkaConsumerFactory {

  private final KafkaProperties kafkaProperties;

  public KafkaConsumerFactory(KafkaProperties kafkaProperties) {
    this.kafkaProperties = kafkaProperties;
  }

  public KafkaConsumer<byte[], byte[]> createConsumer(int instanceId) {
    if (kafkaProperties.isEnableAutoCommit()) {
      throw new IllegalArgumentException("Kafka auto-commit must be disabled for durable processing");
    }

    Map<String, Object> configs = new HashMap<>();
    configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
    configs.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getConsumerGroup());
    configs.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    configs.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaProperties.getAutoOffsetReset());
    configs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, kafkaProperties.getMaxPollRecords());
    configs.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, kafkaProperties.getSessionTimeoutMs());
    configs.put(
        ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, kafkaProperties.getHeartbeatIntervalMs());
    configs.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, kafkaProperties.getMaxPollIntervalMs());
    configs.put(
        ConsumerConfig.CLIENT_ID_CONFIG,
        "consumer-" + kafkaProperties.getConsumerGroup() + "-" + instanceId);

    if (kafkaProperties.getAdditionalConfigs() != null) {
      configs.putAll(kafkaProperties.getAdditionalConfigs());
    }
    configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

    return new KafkaConsumer<>(configs);
  }
}
