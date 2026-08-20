package com.nimbo.batch.config;

import lombok.Getter;
import lombok.Setter;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "spark")
@Getter
@Setter
public class SparkConfig {

    private String master;
    private String appName;
    private String executorMemory;
    private int executorCores;
    private int numExecutors;
    private int shufflePartitions;
    private String driverMemory;

    private Boolean adaptiveEnabled;
    private Boolean adaptiveCoalescePartitionsEnabled;
    private String serializer;
    private Boolean uiEnabled;
    private String ioModeDefault;
    private String driverHost;
    private String driverBindAddress;

    @Bean
    public SparkSession sparkSession() {
        SparkSession.Builder builder = SparkSession.builder()
                .master(master)
                .appName(appName);

        if (executorMemory != null && !executorMemory.isBlank()) {
            builder.config("spark.executor.memory", executorMemory);
        }

        if (executorCores > 0) {
            builder.config("spark.executor.cores", executorCores);
        }

        if (shufflePartitions > 0) {
            builder.config("spark.sql.shuffle.partitions", shufflePartitions);
        }

        if (adaptiveEnabled != null) {
            builder.config("spark.sql.adaptive.enabled", adaptiveEnabled);
        }

        if (adaptiveCoalescePartitionsEnabled != null) {
            builder.config(
                    "spark.sql.adaptive.coalescePartitions.enabled",
                    adaptiveCoalescePartitionsEnabled
            );
        }

        if (serializer != null && !serializer.isBlank()) {
            builder.config("spark.serializer", serializer);
        }

        if (uiEnabled != null) {
            builder.config("spark.ui.enabled", uiEnabled);
        }

        if (ioModeDefault != null && !ioModeDefault.isBlank()) {
            builder.config("spark.io.mode.default", ioModeDefault);
        }

        if (driverHost != null && !driverHost.isBlank()) {
            builder.config("spark.driver.host", driverHost);
        }

        if (driverBindAddress != null && !driverBindAddress.isBlank()) {
            builder.config("spark.driver.bindAddress", driverBindAddress);
        }

        return builder.getOrCreate();
    }
}