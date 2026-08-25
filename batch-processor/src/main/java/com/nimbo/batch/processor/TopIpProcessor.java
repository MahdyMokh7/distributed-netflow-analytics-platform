package com.nimbo.batch.processor;

import com.nimbo.batch.config.HdfsConfig;
import com.nimbo.batch.config.BatchConfig;
import com.nimbo.batch.config.OutputConfig;
import com.nimbo.batch.writer.PostgresWriter;
import com.nimbo.batch.writer.TableMapping;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

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
    public void process(UUID batchId) {
        logHeader("=== TOP IPs BY BANDWIDTH CONSUMPTION (per {} windows) ===", windowDuration);

        try {
            Dataset<Row> aggregated = aggregateTopIps();

            long recordCount = aggregated.count();
            log.info("Total unique IPs (src+dst): {}", recordCount);

            if (recordCount == 0) {
                log.warn("No IP records found in the time window");
                return;
            }

            writeOutput(aggregated, batchId);

            log.info("Total aggregated records: {}", recordCount);

        } catch (Exception e) {
            log.error("Error processing top IP aggregation", e);
            throw new RuntimeException("Top IP aggregation failed", e);
        }
    }

    private Dataset<Row> aggregateTopIps() {
        Dataset<Row> parquetDf = readParquetFiles();

        Dataset<Row> srcPerspective = parquetDf
                .select(
                        col("IPV4_SRC_ADDR").alias("ip"),
                        col("OUT_BYTES").alias("outbound_bytes"),
                        col("IN_BYTES").alias("inbound_bytes"),
                        col("OUT_PKTS").alias("outbound_packets"),
                        col("IN_PKTS").alias("inbound_packets"),
                        col("processing_time")
                );

        Dataset<Row> dstPerspective = parquetDf
                .select(
                        col("IPV4_DST_ADDR").alias("ip"),
                        col("IN_BYTES").alias("outbound_bytes"),
                        col("OUT_BYTES").alias("inbound_bytes"),
                        col("IN_PKTS").alias("outbound_packets"),
                        col("OUT_PKTS").alias("inbound_packets"),
                        col("processing_time")
                );

        Dataset<Row> unified = srcPerspective.unionByName(dstPerspective);

        return unified
                .groupBy(
                        col("ip"),
                        window(col("processing_time"), windowDuration)
                )
                .agg(
                        sum(col("outbound_bytes")).alias("outbound_bytes"),
                        sum(col("inbound_bytes")).alias("inbound_bytes"),
                        sum(col("outbound_packets")).alias("outbound_packets"),
                        sum(col("inbound_packets")).alias("inbound_packets")
                )
                .withColumn("window_start", col("window.start"))
                .withColumn("window_end", col("window.end"))
                .drop("window")
                .orderBy(
                        col("window_start").desc(),
                        col("outbound_bytes").plus(col("inbound_bytes")).desc()
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
        TableMapping mapping = TableMapping.forIpBandwidthAggregates();

        long written = writeToDatabase(aggregated, mapping, batchId);

        log.info("Written {} records to ip_bandwidth_aggregates with batch_id: {}", written, batchId);
    }
}