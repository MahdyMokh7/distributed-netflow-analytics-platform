package com.nimbo.ingester.config;

import jakarta.validation.Validation;
import jakarta.validation.Validator;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
        properties.setConsumerGroup("");

        // When: Jakarta validation evaluates required text fields
        var violations = validator.validate(properties);

        // Then: each missing required property produces one violation
        assertThat(violations)
                .extracting(violation -> violation.getPropertyPath().toString())
                .containsExactlyInAnyOrder("bootstrapServers", "topic", "dlqTopic", "consumerGroup");
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

    private KafkaProperties validProperties() {
        KafkaProperties properties = new KafkaProperties();
        properties.setBootstrapServers("kafka-1:9092,kafka-2:9092");
        properties.setTopic("netflow-v2");
        properties.setDlqTopic("netflow-v2-dlq");
        properties.setConsumerGroup("parquet-writer");
        return properties;
    }
}
