package com.nimbo.ingester;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Nimbo Data Ingester Service
 *
 * This is the first microservice in the Nimbo data processing pipeline.
 * Responsibilities:
 *   1. Consume Netflow data from Kafka topic
 *   2. Deserialize Avro messages
 *   3. Write data as Parquet files to HDFS
 *
 * The service is designed for:
 *   - Zero data loss (at-least-once delivery semantics)
 *   - Horizontal scalability
 *   - Automated deployment via Jenkins
 *
 * @author Nimbo Team
 * @version 1.0.0
 */
@SpringBootApplication
@EnableConfigurationProperties
@EnableScheduling  // For scheduled file rolling
public class DataIngesterApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataIngesterApplication.class, args);
    }
}
