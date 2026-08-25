package com.nimbo.batch.processor;

import com.nimbo.batch.writer.PostgresWriter;
import com.nimbo.batch.writer.TableMapping;
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
        return df.withColumn("processing_time", current_timestamp());
    }

    protected void logSeparator() {
        log.info("{}", "=".repeat(80));
    }

    protected void logHeader(String title, Object... args) {
        logSeparator();
        log.info(title, args);
        logSeparator();
    }

    protected long writeToDatabase(Dataset<Row> dataset, TableMapping mapping, UUID batchId) {
        long startTime = System.currentTimeMillis();
        long recordCount = dataset.count();

        log.info("Writing {} records to table: {} with batch_id: {}", recordCount, mapping.getTableName(), batchId);

        try {
            long written = postgresWriter.write(dataset, mapping, batchId);
            long duration = System.currentTimeMillis() - startTime;

            log.info("Successfully wrote {} records to table: {} in {} ms", written, mapping.getTableName(), duration);

            return written;
        } catch (Exception e) {
            log.error("Failed to write to table: {} with batch_id: {}", mapping.getTableName(), batchId, e);
            throw new RuntimeException("Database write failed for table: " + mapping.getTableName(), e);
        }
    }

    public abstract void process(UUID batchId);
}