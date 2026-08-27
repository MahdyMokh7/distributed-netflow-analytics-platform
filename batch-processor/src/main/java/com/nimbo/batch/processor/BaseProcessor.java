package com.nimbo.batch.processor;

import com.nimbo.batch.writer.PostgresWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.UUID;

import static org.apache.spark.sql.functions.current_timestamp;

@Slf4j
public abstract class BaseProcessor {

    protected final SparkSession spark;
    protected final String parquetPath;
    protected final String windowDuration;
    protected final PostgresWriter postgresWriter;

    protected BaseProcessor(SparkSession spark,
                            String parquetPath,
                            String windowDuration,
                            PostgresWriter postgresWriter) {
        this.spark = spark;
        this.parquetPath = parquetPath;
        this.windowDuration = windowDuration;
        this.postgresWriter = postgresWriter;
    }

    protected Dataset<Row> readParquetFiles() {
        log.info("Reading Parquet files from: {}", parquetPath);
        Dataset<Row> df = spark.read().parquet(parquetPath);
        return df ;
    }

    protected void logSeparator() {
        log.info("{}", "=".repeat(80));
    }

    protected void logHeader(String title, Object... args) {
        logSeparator();
        log.info(title, args);
        logSeparator();
    }

    public abstract void process(UUID batchId);
}