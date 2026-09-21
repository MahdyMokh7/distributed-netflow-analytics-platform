package com.nimbo.batch.processor;

import com.nimbo.batch.config.BatchConfig;
import com.nimbo.batch.config.OutputConfig;
import com.nimbo.batch.exception.BatchProcessingException;
import com.nimbo.batch.metrics.BatchProcessorMetrics.Stage;
import com.nimbo.batch.writer.PostgresWriter;
import static com.nimbo.batch.constants.ColumnConstants.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.UUID;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.window;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.sha2;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.timestamp_micros;

@Component
@Slf4j
@Order(2)
public class ConnectionProcessor extends BaseProcessor {

    private static final int DISPLAY_LIMIT = 20;

    private final OutputConfig outputConfig;

    public ConnectionProcessor(
            BatchConfig batchConfig,
            OutputConfig outputConfig,
            PostgresWriter postgresWriter) {

        super(batchConfig.getWindowDuration(), postgresWriter);

        this.outputConfig = outputConfig;
    }

    @Override
    public Stage metricStage() {
        return Stage.CONNECTION;
    }

    @Override
    public void process(Dataset<Row> sourceDf, UUID batchId) {
        logHeader("=== CONNECTION AGGREGATION (IP Connections {} Windows) ===", windowDuration);

        try {
            Dataset<Row> aggregated = aggregateConnections(sourceDf);

            long recordCount = aggregated.count();
            log.info("Total connection aggregation records: {}", recordCount);

            if (recordCount == 0) {
                log.warn("No connection records found in the time window");
                return;
            }

            writeOutput(aggregated, batchId);

        } catch (RuntimeException e) {
            log.error("Error processing connection aggregation", e);

            throw new BatchProcessingException(batchId, "Connection aggregation failed", e);
        }
    }

    private Dataset<Row> aggregateConnections(Dataset<Row> sourceDf) {
        return sourceDf
                .groupBy(
                        col(NETFLOW_SRC_IP).alias(SPARK_SRC_IP),
                        col(NETFLOW_DST_IP).alias(SPARK_DST_IP),
                        window(timestamp_micros(col(NETFLOW_EVENT_TIME)), windowDuration)
                )
                .agg(
                        sum(col(NETFLOW_IN_BYTES)).alias(SPARK_IN_BYTES),
                        sum(col(NETFLOW_OUT_BYTES)).alias(SPARK_OUT_BYTES),
                        sum(col(NETFLOW_IN_PKTS)).alias(SPARK_IN_PACKETS),
                        sum(col(NETFLOW_OUT_PKTS)).alias(SPARK_OUT_PACKETS)
                )
                .withColumn(SPARK_WINDOW_START, col("window.start"))
                .withColumn(SPARK_WINDOW_END, col("window.end"))
                .drop("window")
                .withColumn(SPARK_ID, sha2(concat_ws("|",
                        col(SPARK_SRC_IP),
                        col(SPARK_DST_IP),
                        col(SPARK_WINDOW_START).cast("string")
                ), 256));
    }

    private void writeOutput(Dataset<Row> aggregated, UUID batchId) {
        if (outputConfig.isConsoleMode()) {
            writeToConsole(aggregated);
        }

        if (outputConfig.isDatabaseMode()) {
            writeToDatabase(aggregated, batchId);
        }
    }

    private void writeToConsole(Dataset<Row> aggregated) {
        log.info("Top 20 Connection Aggregations in Console ({} windows):", windowDuration);
        log.info("Id | Src IP | Dst IP | Window Start | Window End | In Bytes | Out Bytes | In Packets | Out Packets");
        log.info("-".repeat(110));

        aggregated.show(DISPLAY_LIMIT, false);
    }

    private void writeToDatabase(Dataset<Row> aggregated, UUID batchId) {
        long written = postgresWriter.writeConnectionAggregates(aggregated, batchId);
        log.info("Written {} records to connection_aggregates with batch_id: {}", written, batchId);
    }
}
