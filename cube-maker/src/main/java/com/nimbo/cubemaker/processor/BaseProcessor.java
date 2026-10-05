package com.nimbo.cubemaker.processor;

import com.nimbo.cubemaker.metrics.CubeMakerMetrics.Stage;
import com.nimbo.cubemaker.writer.PostgresWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.UUID;

@Slf4j
public abstract class BaseProcessor {

    protected final String windowDuration;
    protected final PostgresWriter postgresWriter;

    protected BaseProcessor(String windowDuration,
                            PostgresWriter postgresWriter) {
        this.windowDuration = windowDuration;
        this.postgresWriter = postgresWriter;
    }

    protected void logSeparator() {
        log.info("{}", "=".repeat(80));
    }

    protected void logHeader(String title, Object... args) {
        logSeparator();
        log.info(title, args);
        logSeparator();
    }

    public abstract Stage metricStage();

    public abstract void process(Dataset<Row> sourceDf, UUID batchId);
}
