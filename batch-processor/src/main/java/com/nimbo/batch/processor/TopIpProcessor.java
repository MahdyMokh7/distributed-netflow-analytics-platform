package com.nimbo.batch.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.*;

@Component
@Slf4j
public class TopIpProcessor extends BaseProcessor {

    private static final int TOP_N = 20;

    public TopIpProcessor(SparkSession spark,
                          com.nimbo.batch.config.HdfsConfig hdfsConfig) {
        super(spark, hdfsConfig.getFullParquetPath());
    }

    @Override
    public void process() {
        logHeader("=== TOP IPs BY BANDWIDTH CONSUMPTION ===");

        try {
            Dataset<Row> parquetDf = readParquetFiles();

            Dataset<Row> srcIpTraffic = parquetDf
                    .groupBy(col("IPV4_SRC_ADDR").alias("ip"))
                    .agg(
                            sum(col("OUT_BYTES")).alias("outbound_bytes"),
                            sum("IN_BYTES").alias("inbound_bytes"),
                            sum(col("IN_BYTES").plus(col("OUT_BYTES"))).alias("total_bytes")
                    )
                    .orderBy(col("total_bytes").desc());

            long recordCount = srcIpTraffic.count();
            log.info("Total unique source IPs: {}", recordCount);

            if (recordCount == 0) {
                log.warn("No IP records found in the time window");
                return;
            }

            log.info("Top {} Source IPs by Bandwidth Consumption:", TOP_N);
            log.info("Rank | IP | Total Bytes | Inbound Bytes | Outbound Bytes");
            log.info("-".repeat(80));

            Dataset<Row> ranked = srcIpTraffic
                    .withColumn("rank", row_number().over(org.apache.spark.sql.expressions.Window.orderBy(col("total_bytes").desc())))
                    .filter(col("rank").leq(TOP_N))
                    .orderBy(col("rank"));

            ranked.show(TOP_N, false);

            log.info("Total unique source IPs: {}", recordCount);

        } catch (Exception e) {
            log.error("Error processing top IP aggregation", e);
            throw new RuntimeException("Top IP aggregation failed", e);
        }
    }
}