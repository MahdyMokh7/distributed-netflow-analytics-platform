package com.nimbo.ingester.config;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.validation.Validation;
import jakarta.validation.Validator;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class KafkaPropertiesTest {

  private static Validator validator;

  @BeforeAll
  static void createValidator() {
    validator = Validation.buildDefaultValidatorFactory().getValidator();
  }

  @Test
  void givenProductionLikeKafkaProperties_whenValidated_thenConfigurationIsAccepted() {
    // Given: complete Kafka settings with deterministic additional configuration
    KafkaProperties properties = validProperties();
    properties.setAdditionalConfigs(Map.of("fetch.min.bytes", "1024"));

    // When: Jakarta validation evaluates the configuration
    var violations = validator.validate(properties);

    // Then: all values satisfy the declared application constraints
    assertThat(violations).isEmpty();
    assertThat(properties.getAdditionalConfigs()).containsEntry("fetch.min.bytes", "1024");
  }

  @Test
  void givenBlankRequiredKafkaNames_whenValidated_thenEveryRequiredPropertyIsRejected() {
    // Given: blank connection, topic, DLQ, and group identifiers
    KafkaProperties properties = validProperties();
    properties.setBootstrapServers(" ");
    properties.setTopic("");
    properties.setDlqTopic(" ");
    properties.setLateTopic(" ");
    properties.setConsumerGroup("");

    // When: Jakarta validation evaluates required text fields
    var violations = validator.validate(properties);

    // Then: each missing required property produces one violation
    assertThat(violations)
        .extracting(violation -> violation.getPropertyPath().toString())
        .containsExactlyInAnyOrder(
            "bootstrapServers", "topic", "dlqTopic", "lateTopic", "consumerGroup");
  }

  @Test
  void givenValuesBelowOperationalMinimums_whenValidated_thenEveryBoundaryIsRejected() {
    // Given: Kafka timing and capacity values immediately below supported limits
    KafkaProperties properties = validProperties();
    properties.setPollTimeoutMs(99);
    properties.setMaxPollRecords(0);
    properties.setSessionTimeoutMs(999);
    properties.setHeartbeatIntervalMs(998);
    properties.setMaxPollIntervalMs(9_999);
    properties.setConsumerInstances(0);

    // When: Jakarta validation evaluates numerical boundaries
    var violations = validator.validate(properties);

    // Then: every unsafe numerical value is rejected
    assertThat(violations)
        .extracting(violation -> violation.getPropertyPath().toString())
        .containsExactlyInAnyOrder(
            "pollTimeoutMs",
            "maxPollRecords",
            "sessionTimeoutMs",
            "heartbeatIntervalMs",
            "maxPollIntervalMs",
            "consumerInstances");
  }

  @Test
  void givenValuesAtValidOperationalBoundaries_whenValidated_thenConfigurationIsAccepted() {
    // Given: Kafka values at valid individual and relational boundaries
    KafkaProperties properties = validProperties();
    properties.setPollTimeoutMs(100);
    properties.setMaxPollRecords(1);
    properties.setSessionTimeoutMs(1_001);
    properties.setHeartbeatIntervalMs(1_000);
    properties.setMaxPollIntervalMs(10_000);
    properties.setConsumerInstances(1);

    // When: Jakarta validation evaluates inclusive numerical boundaries
    var violations = validator.validate(properties);

    // Then: minimum values with a lower heartbeat remain valid
    assertThat(violations).isEmpty();
  }

  @Test
  void givenHeartbeatEqualToSessionTimeout_whenValidated_thenTimingRelationshipIsRejected() {
    // Given: heartbeat and session timeout use the same duration
    KafkaProperties properties = validProperties();
    properties.setSessionTimeoutMs(1_000);
    properties.setHeartbeatIntervalMs(1_000);

    // When: Jakarta validation evaluates the timing relationship
    var violations = validator.validate(properties);

    // Then: an equal heartbeat and session timeout is rejected
    assertThat(violations)
        .extracting(violation -> violation.getMessage())
        .containsExactly("heartbeatIntervalMs must be less than sessionTimeoutMs");
  }

  @Test
  void givenNoDlqPublishTimeoutOverride_whenCreated_thenDefaultIsTenSeconds() {
    // Given: Kafka properties that do not configure the DLQ publish timeout
    KafkaProperties properties = validProperties();

    // When: Jakarta validation evaluates the defaults
    var violations = validator.validate(properties);

    // Then: the documented default is used and accepted
    assertThat(properties.getDlqPublishTimeoutMs()).isEqualTo(10_000L);
    assertThat(violations).isEmpty();
  }

  @Test
  void givenDlqPublishTimeoutAtMinimum_whenValidated_thenConfigurationIsAccepted() {
    // Given: the smallest supported DLQ publish timeout
    KafkaProperties properties = validProperties();
    properties.setDlqPublishTimeoutMs(100);

    // When: Jakarta validation evaluates the inclusive lower bound
    var violations = validator.validate(properties);

    // Then: the boundary value is valid and readable through the getter
    assertThat(violations).isEmpty();
    assertThat(properties.getDlqPublishTimeoutMs()).isEqualTo(100L);
  }

  @ParameterizedTest
  @ValueSource(longs = {99L, 1L, 0L, -1L})
  void givenDlqPublishTimeoutBelowMinimum_whenValidated_thenOnlyThatPropertyIsRejected(
      long timeoutMs) {
    // Given: a DLQ publish timeout below the supported minimum
    KafkaProperties properties = validProperties();
    properties.setDlqPublishTimeoutMs(timeoutMs);

    // When: Jakarta validation evaluates the timeout
    var violations = validator.validate(properties);

    // Then: exactly the timeout property is reported
    assertThat(violations)
        .extracting(violation -> violation.getPropertyPath().toString())
        .containsExactly("dlqPublishTimeoutMs");
  }

  private KafkaProperties validProperties() {
    KafkaProperties properties = new KafkaProperties();
    properties.setBootstrapServers("kafka-1:9092,kafka-2:9092");
    properties.setTopic("netflow-v2");
    properties.setDlqTopic("netflow-v2-dlq");
    properties.setLateTopic("netflow-v2-late");
    properties.setConsumerGroup("parquet-writer");
    return properties;
  }
}
