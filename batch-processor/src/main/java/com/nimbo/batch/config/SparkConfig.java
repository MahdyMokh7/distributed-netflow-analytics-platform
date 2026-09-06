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
    private int maxCores;
    private int shufflePartitions;
    private String driverMemory;

    private Boolean adaptiveEnabled;
    private Boolean adaptiveCoalescePartitionsEnabled;
    private String serializer;
    private Boolean uiEnabled;
    private String ioModeDefault;
    private String driverHost;
    private String driverBindAddress;

    private Boolean speculation;
    private String jarsPath;
    private String executorExtraJavaOptions;

    @Bean
    public SparkSession sparkSession() {
        SparkSession.Builder builder = SparkSession.builder()
                .master(master)
                .appName(appName);

        setIfPresent(builder, "spark.executor.memory", executorMemory);
        setIfPositive(builder, "spark.executor.cores", executorCores);
        setIfPositive(builder, "spark.executor.instances", numExecutors);
        setIfPositive(builder, "spark.cores.max", maxCores);
        setIfPositive(builder, "spark.sql.shuffle.partitions", shufflePartitions);

        setIfPresent(builder, "spark.sql.adaptive.enabled", adaptiveEnabled);
        setIfPresent(builder, "spark.sql.adaptive.coalescePartitions.enabled", adaptiveCoalescePartitionsEnabled);

        setIfPresent(builder, "spark.serializer", serializer);
        setIfPresent(builder, "spark.ui.enabled", uiEnabled);
        setIfPresent(builder, "spark.io.mode.default", ioModeDefault);
        setIfPresent(builder, "spark.driver.host", driverHost);
        setIfPresent(builder, "spark.driver.bindAddress", driverBindAddress);

        setIfPresent(builder, "spark.speculation", speculation);
        setIfPresent(builder, "spark.jars", jarsPath);
        setIfPresent(builder, "spark.executor.extraJavaOptions", executorExtraJavaOptions);

        return builder.getOrCreate();
    }

    private void setIfPresent(
            SparkSession.Builder builder,
            String key,
            String value
    ) {
        if (value != null && !value.isBlank()) {
            builder.config(key, value);
        }
    }

    private void setIfPresent(
            SparkSession.Builder builder,
            String key,
            Boolean value
    ) {
        if (value != null) {
            builder.config(key, value);
        }
    }

    private void setIfPositive(
            SparkSession.Builder builder,
            String key,
            int value
    ) {
        if (value > 0) {
            builder.config(key, value);
        }
    }
}
