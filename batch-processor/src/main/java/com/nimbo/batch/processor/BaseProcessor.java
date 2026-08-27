package com.nimbo.batch.processor;

import com.nimbo.batch.writer.PostgresWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.hadoop.fs.Path;
import java.util.List;

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

    protected Dataset<Row> readParquetFiles(List<Path> files) {
        if (files == null || files.isEmpty()) {
            log.warn("No files provided to read");
            return spark.emptyDataFrame();
        }

        String[] paths = files.stream()
                .map(Path::toString)
                .toArray(String[]::new);

        log.info("Reading {} Parquet files from: {}", paths.length, paths[0]);
        return spark.read().parquet(paths);
    }

    protected void logSeparator() {
        log.info("{}", "=".repeat(80));
    }

    protected void logHeader(String title, Object... args) {
        logSeparator();
        log.info(title, args);
        logSeparator();
    }

    public abstract void process(List<Path> files, UUID batchId);}