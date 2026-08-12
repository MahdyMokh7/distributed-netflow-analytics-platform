package com.nimbo.ingester.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

import java.util.Map;

@Component
@Validated
@ConfigurationProperties(prefix = "ingester.kafka")
public class KafkaProperties {

    @NotBlank
    private String bootstrapServers;

    @NotBlank
    private String topic;

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

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public boolean isEnableAutoCommit() {
        return enableAutoCommit;
    }

    public void setEnableAutoCommit(boolean enableAutoCommit) {
        this.enableAutoCommit = enableAutoCommit;
    }

    public int getPollTimeoutMs() {
        return pollTimeoutMs;
    }

    public void setPollTimeoutMs(int pollTimeoutMs) {
        this.pollTimeoutMs = pollTimeoutMs;
    }

    public int getMaxPollRecords() {
        return maxPollRecords;
    }

    public void setMaxPollRecords(int maxPollRecords) {
        this.maxPollRecords = maxPollRecords;
    }

    public int getSessionTimeoutMs() {
        return sessionTimeoutMs;
    }

    public void setSessionTimeoutMs(int sessionTimeoutMs) {
        this.sessionTimeoutMs = sessionTimeoutMs;
    }

    public int getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    public void setHeartbeatIntervalMs(int heartbeatIntervalMs) {
        this.heartbeatIntervalMs = heartbeatIntervalMs;
    }

    public int getMaxPollIntervalMs() {
        return maxPollIntervalMs;
    }

    public void setMaxPollIntervalMs(int maxPollIntervalMs) {
        this.maxPollIntervalMs = maxPollIntervalMs;
    }

    public Map<String, String> getAdditionalConfigs() {
        return additionalConfigs;
    }

    public void setAdditionalConfigs(Map<String, String> additionalConfigs) {
        this.additionalConfigs = additionalConfigs;
    }
}