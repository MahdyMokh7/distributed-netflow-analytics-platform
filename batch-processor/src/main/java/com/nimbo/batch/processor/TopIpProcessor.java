package com.nimbo.batch.processor;

import com.nimbo.batch.config.HdfsConfig;
import com.nimbo.batch.config.BatchConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.window;
import static org.apache.spark.sql.functions.sum;

@Component
@Slf4j
@Order(3)
public class TopIpProcessor extends BaseProcessor {

    private static final int TOP_N = 20;

    public TopIpProcessor(SparkSession spark,
                          HdfsConfig hdfsConfig,
                          BatchConfig batchConfig) {
        super(spark, hdfsConfig.getFullParquetPath(), batchConfig.getWindowDuration());
    }

    @Override
    public void process() {
        logHeader("=== TOP IPs BY BANDWIDTH CONSUMPTION (per {} windows) ===", windowDuration);
        try {
            Dataset<Row> parquetDf = readParquetFiles();

            // Src-perspective
            Dataset<Row> srcPerspective = parquetDf
                    .select(
                            col("IPV4_SRC_ADDR").alias("ip"),
                            col("OUT_BYTES").alias("outbound_bytes"),
                            col("IN_BYTES").alias("inbound_bytes"),
                            col("OUT_PKTS").alias("outbound_packets"),
                            col("IN_PKTS").alias("inbound_packets"),
                            col("processing_time")
                    );

            // Dst-perspective
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

            Dataset<Row> aggregated = unified
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
                    .orderBy(col("window_start").desc(),
                            col("outbound_bytes").plus(col("inbound_bytes")).desc());

            long recordCount = aggregated.count();
            log.info("Total unique IPs (src+dst): {}", recordCount);

            if (recordCount == 0) {
                log.warn("No IP records found in the time window");
                return;
            }

            log.info("Top {} IPs by Bandwidth Consumption (per {} windows):", TOP_N, windowDuration);
            log.info("IP | Window Start | Window End | Outbound Bytes | Inbound Bytes | Outbound Packets | Inbound Packets");
            log.info("-".repeat(140));

            aggregated.show(TOP_N, false);
            log.info("Total aggregated records: {}", recordCount);

        } catch (Exception e) {
            log.error("Error processing top IP aggregation", e);
            throw new RuntimeException("Top IP aggregation failed", e);
        }
    }
}