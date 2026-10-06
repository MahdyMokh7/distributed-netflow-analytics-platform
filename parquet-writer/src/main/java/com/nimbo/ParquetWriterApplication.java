package com.nimbo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Nimbo Parquet Writer Service
 *
 * <p>This is the main entry point for the Nimbo data processing pipeline. The service consumes
 * Netflow data from Kafka, deserializes Avro records, and writes them as Parquet files to HDFS.
 *
 * <p>Responsibilities: - Kafka consumption and Avro deserialization - HDFS Parquet writing and
 * batch management
 *
 * @author Nimbo Team
 */
@SpringBootApplication
@EnableConfigurationProperties
@EnableScheduling
public class ParquetWriterApplication {

  public static void main(String[] args) {
    System.setProperty("HADOOP_USER_NAME", "hadoop");
    SpringApplication.run(ParquetWriterApplication.class, args);
  }
}
