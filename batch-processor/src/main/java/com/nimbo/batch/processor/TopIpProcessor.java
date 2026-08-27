package com.nimbo.batch.processor;

import com.nimbo.batch.config.HdfsConfig;
import com.nimbo.batch.config.BatchConfig;
import com.nimbo.batch.config.OutputConfig;
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

@Component
@Slf4j
@Order(3)
public class TopIpProcessor extends BaseProcessor {

    private static final int TOP_N = 20;

    private final OutputConfig outputConfig;

    public TopIpProcessor(SparkSession spark,
                          HdfsConfig hdfsConfig,
                          BatchConfig batchConfig,
                          OutputConfig outputConfig,
                          PostgresWriter postgresWriter) {
        super(spark, hdfsConfig.getFullParquetPath(), batchConfig.getWindowDuration(), postgresWriter);
        this.outputConfig = outputConfig;
    }

    @Override
    public void process(List<Path> files, UUID batchId) {
        logHeader("=== TOP IPs BY BANDWIDTH CONSUMPTION (per {} windows) ===", windowDuration);

        try {
            if (files == null || files.isEmpty()) {
                log.warn("No files provided, skipping connection aggregation");
                return;
            }

            Dataset<Row> aggregated = aggregateTopIps(files);

            long recordCount = aggregated.count();
            log.info("Total unique IPs (src+dst): {}", recordCount);

            if (recordCount == 0) {
                log.warn("No IP records found in the time window");
                return;
            }

            writeOutput(aggregated, batchId);

        } catch (Exception e) {
            log.error("Error processing top IP aggregation", e);
            throw new RuntimeException("Top IP aggregation failed", e);
        }
    }

    private Dataset<Row> aggregateTopIps(List<Path> files) {
        Dataset<Row> parquetDf = readParquetFiles(files);

        Dataset<Row> srcPerspective = parquetDf
                .select(
                        col(NETFLOW_SRC_IP).alias(SPARK_IP),
                        col(NETFLOW_OUT_BYTES).alias(SPARK_OUTBOUND_BYTES),
                        col(NETFLOW_IN_BYTES).alias(SPARK_INBOUND_BYTES),
                        col(NETFLOW_OUT_PKTS).alias(SPARK_OUTBOUND_PACKETS),
                        col(NETFLOW_IN_PKTS).alias(SPARK_INBOUND_PACKETS),
                        col(NETFLOW_PROCESSING_TIME)
                );

        Dataset<Row> dstPerspective = parquetDf
                .select(
                        col(NETFLOW_DST_IP).alias(SPARK_IP),
                        col(NETFLOW_IN_BYTES).alias(SPARK_OUTBOUND_BYTES),
                        col(NETFLOW_OUT_BYTES).alias(SPARK_INBOUND_BYTES),
                        col(NETFLOW_IN_PKTS).alias(SPARK_OUTBOUND_PACKETS),
                        col(NETFLOW_OUT_PKTS).alias(SPARK_INBOUND_PACKETS),
                        col(NETFLOW_PROCESSING_TIME)
                );

        Dataset<Row> unified = srcPerspective.unionByName(dstPerspective);

        return unified
                .groupBy(
                        col(SPARK_IP),
                        window(col(NETFLOW_PROCESSING_TIME), windowDuration)
                )
                .agg(
                        sum(col(SPARK_OUTBOUND_BYTES)).alias(SPARK_OUTBOUND_BYTES),
                        sum(col(SPARK_INBOUND_BYTES)).alias(SPARK_INBOUND_BYTES),
                        sum(col(SPARK_OUTBOUND_PACKETS)).alias(SPARK_OUTBOUND_PACKETS),
                        sum(col(SPARK_INBOUND_PACKETS)).alias(SPARK_INBOUND_PACKETS)
                )
                .withColumn(SPARK_WINDOW_START, col("window.start"))
                .withColumn(SPARK_WINDOW_END, col("window.end"))
                .drop("window")
                .orderBy(
                        col(SPARK_WINDOW_START).desc(),
                        col(SPARK_OUTBOUND_BYTES).plus(col(SPARK_INBOUND_BYTES)).desc()
                );
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
        log.info("Top {} IPs by Bandwidth Consumption (per {} windows):", TOP_N, windowDuration);
        log.info("IP | Window Start | Window End | Outbound Bytes | " +
                "Inbound Bytes | Outbound Packets | Inbound Packets");
        log.info("-".repeat(140));

        aggregated.show(TOP_N, false);
    }

    private void writeToDatabase(Dataset<Row> aggregated, UUID batchId) {
        long written = postgresWriter.writeIpBandwidthAggregates(aggregated, batchId);
        log.info("Written {} records to ip_bandwidth_aggregates with batch_id: {}", written, batchId);
    }
}