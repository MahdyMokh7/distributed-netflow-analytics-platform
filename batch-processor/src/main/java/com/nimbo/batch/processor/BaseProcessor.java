package com.nimbo.batch.processor;

import com.nimbo.batch.config.BatchConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static org.apache.spark.sql.functions.current_timestamp;

@Slf4j
public abstract class BaseProcessor {

    protected final SparkSession spark;
    protected final String parquetPath;
    protected final String windowDuration;

    private static final DateTimeFormatter TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    protected BaseProcessor(SparkSession spark, String parquetPath, String windowDuration) {
        this.spark = spark;
        this.parquetPath = parquetPath;
        this.windowDuration = windowDuration;
    }

    protected Dataset<Row> readParquetFiles() {
        log.info("Reading Parquet files from: {}", parquetPath);
        Dataset<Row> df = spark.read().parquet(parquetPath);

        return df.withColumn("processing_time", current_timestamp());
    }

    protected String formatTimestamp(LocalDateTime timestamp) {
        return timestamp != null ? timestamp.format(TIME_FORMATTER) : "N/A";
    }

    protected void logSeparator() {
        log.info("{}", "=".repeat(80));
    }

    protected void logHeader(String title, Object... args) {
        logSeparator();
        log.info(title, args);
        logSeparator();
    }

    public abstract void process();
}