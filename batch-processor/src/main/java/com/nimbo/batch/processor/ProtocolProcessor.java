package com.nimbo.batch.processor;

import com.nimbo.batch.config.HdfsConfig;
import com.nimbo.batch.config.BatchConfig;
import com.nimbo.batch.config.OutputConfig;
import com.nimbo.batch.exception.BatchProcessingException;
import com.nimbo.batch.writer.PostgresWriter;
import static com.nimbo.batch.constants.ColumnConstants.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.window;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.timestamp_micros;

@Component
@Slf4j
@Order(4)
public class ProtocolProcessor extends BaseProcessor {

    private static final int DISPLAY_LIMIT = 20;

    private final OutputConfig outputConfig;

    public ProtocolProcessor(SparkSession spark,
                             HdfsConfig hdfsConfig,
                             BatchConfig batchConfig,
                             OutputConfig outputConfig,
                             PostgresWriter postgresWriter) {
        super(spark, hdfsConfig.getFullParquetPath(), batchConfig.getWindowDuration(), postgresWriter);
        this.outputConfig = outputConfig;
    }

    @Override
    public void process(List<Path> files, UUID batchId) {
        logHeader("=== PROTOCOL STATISTICS (Protocol Usage {} Windows) ===", windowDuration);

        try {
            if (files == null || files.isEmpty()) {
                log.warn("No files provided, skipping Protocol aggregation");
                return;
            }

            Dataset<Row> aggregated = aggregateProtocols(files);

            long recordCount = aggregated.count();
            log.info("Total protocol aggregation records: {}", recordCount);

            if (recordCount == 0) {
                log.warn("No protocol records found in the time window");
                return;
            }

            writeOutput(aggregated, batchId);

        } catch (Exception e) {
            log.error("Error processing protocol aggregation", e);
            throw new BatchProcessingException(batchId, "Protocol aggregation failed", e);
        }
    }

    private Dataset<Row> aggregateProtocols(List<Path> files) {
        Dataset<Row> parquetDf = readParquetFiles(files);

        return parquetDf
                .groupBy(
                        col(NETFLOW_L7_PROTO).alias(SPARK_L7_PROTOCOL),
                        window(timestamp_micros(col(NETFLOW_PROCESSING_TIME)), windowDuration)
                )
                .agg(
                        sum(col(NETFLOW_IN_BYTES)).alias(SPARK_IN_BYTES),
                        sum(col(NETFLOW_OUT_BYTES)).alias(SPARK_OUT_BYTES),
                        sum(col(NETFLOW_IN_PKTS)).alias(SPARK_IN_PACKETS),
                        sum(col(NETFLOW_OUT_PKTS)).alias(SPARK_OUT_PACKETS),
                        count("*").alias(SPARK_CONNECTION_COUNT)
                )
                .withColumn(SPARK_WINDOW_START, col("window.start"))
                .withColumn(SPARK_WINDOW_END, col("window.end"))
                .drop("window");
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
        log.info("Top 20 Protocol Statistics (by usage):");
        log.info("L7 Proto | Window Start | Window End | In Bytes | " +
                "Out Bytes | In Packets | Out Packets | Connection Count");
        log.info("-".repeat(140));

        aggregated.show(DISPLAY_LIMIT, false);
    }

    private void writeToDatabase(Dataset<Row> aggregated, UUID batchId) {
        long written = postgresWriter.writeProtocolAggregates(aggregated, batchId);
        log.info("Written {} records to protocol_usage_aggregates with batch_id: {}", written, batchId);
    }
}