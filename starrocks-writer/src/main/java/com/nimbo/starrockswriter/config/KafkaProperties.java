package com.nimbo.starrockswriter.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Getter
@Setter
@Validated
@ConfigurationProperties(prefix = "starrockswriter.kafka")
public class KafkaProperties {

    @NotBlank
    private String bootstrapServers;

    @NotBlank
    private String topic;

    @NotBlank
    private String consumerGroup;

    @NotBlank
    private String clientId;

    @NotBlank
    private String autoOffsetReset;

    @Min(1)
    private int maxPollRecords = 1000;

    @Min(100)
    private long pollTimeoutMs = 1000;

    @Min(1048576)
    private int maxPartitionFetchBytes;

    @Min(1)
    private int consumerCount;

    @NotBlank
    private String dlqTopic = "netflow-dlq";

    @Min(100)
    private long dlqPublishTimeoutMs = 10000;
}
