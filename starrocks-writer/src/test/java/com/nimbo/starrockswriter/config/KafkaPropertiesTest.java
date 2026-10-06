package com.nimbo.starrockswriter.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class KafkaPropertiesTest {

  @Test
  void shouldStoreKafkaConnectionProperties() {
    KafkaProperties properties = new KafkaProperties();

    properties.setBootstrapServers("localhost:9092");
    properties.setTopic("multi-version-netflow");
    properties.setConsumerGroup("starrockswriter-group");
    properties.setClientId("worker-1");
    properties.setAutoOffsetReset("earliest");

    assertThat(properties.getBootstrapServers()).isEqualTo("localhost:9092");
    assertThat(properties.getTopic()).isEqualTo("multi-version-netflow");
    assertThat(properties.getConsumerGroup()).isEqualTo("starrockswriter-group");
    assertThat(properties.getClientId()).isEqualTo("worker-1");
    assertThat(properties.getAutoOffsetReset()).isEqualTo("earliest");
  }

  @Test
  void shouldHaveCorrectDefaultValues() {
    KafkaProperties properties = new KafkaProperties();

    assertThat(properties.getMaxPollRecords()).isEqualTo(1000);
    assertThat(properties.getPollTimeoutMs()).isEqualTo(1000);
    assertThat(properties.getDlqTopic()).isEqualTo("netflow-dlq");
    assertThat(properties.getDlqPublishTimeoutMs()).isEqualTo(10000);
  }

  @Test
  void shouldStoreConsumerLimits() {
    KafkaProperties properties = new KafkaProperties();

    properties.setMaxPollRecords(500);
    properties.setPollTimeoutMs(5000);
    properties.setMaxPartitionFetchBytes(2097152);
    properties.setConsumerCount(4);
    properties.setDlqTopic("test-netflow-dlq");
    properties.setDlqPublishTimeoutMs(1500);

    assertThat(properties.getMaxPollRecords()).isEqualTo(500);
    assertThat(properties.getPollTimeoutMs()).isEqualTo(5000);
    assertThat(properties.getMaxPartitionFetchBytes()).isEqualTo(2097152);
    assertThat(properties.getConsumerCount()).isEqualTo(4);
    assertThat(properties.getDlqTopic()).isEqualTo("test-netflow-dlq");
    assertThat(properties.getDlqPublishTimeoutMs()).isEqualTo(1500);
  }
}
