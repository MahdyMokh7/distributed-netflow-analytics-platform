package com.nimbo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Nimbo Data Ingester Service
 *
 * This is the main entry point for the Nimbo data processing pipeline.
 * The service consumes Netflow data from Kafka, deserializes Avro records,
 * and writes them as Parquet files to HDFS.
 *
 * Modules:
 *   - Ingester: Kafka consumption and Avro deserialization
 *   - Storage: HDFS Parquet writing and batch management
 *
 * @author Nimbo Team
 * @version 1.0.0
 */
@SpringBootApplication
@EnableConfigurationProperties
@EnableScheduling
public class Application {

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        SpringApplication.run(Application.class, args);
    }
}