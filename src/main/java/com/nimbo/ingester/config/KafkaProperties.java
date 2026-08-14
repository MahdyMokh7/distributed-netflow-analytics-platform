package com.nimbo.ingester.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.util.Map;


@Setter
@Getter
@Validated
@ConfigurationProperties(prefix = "ingester.kafka")
public class KafkaProperties {

    @NotBlank
    private String bootstrapServers;

    @NotBlank
    private String topic;

    @NotBlank
    private String dlqTopic;

    @NotBlank
    private String consumerGroup;

    private boolean enableAutoCommit = false;

    @Min(100)
    private int pollTimeoutMs = 1000;

    @Min(1)
    private int maxPollRecords = 500;

    @Min(1000)
    private int sessionTimeoutMs = 30000;

    @Min(1000)
    private int heartbeatIntervalMs = 3000;

    @Min(10000)
    private int maxPollIntervalMs = 300000;

    private Map<String, String> additionalConfigs;

}