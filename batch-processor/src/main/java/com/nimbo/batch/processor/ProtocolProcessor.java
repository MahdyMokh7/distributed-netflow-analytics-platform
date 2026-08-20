package com.nimbo.batch.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.*;

@Component
@Slf4j
public class ProtocolProcessor extends BaseProcessor {

    private static final String HOUR_WINDOW = "1 hour";

    public ProtocolProcessor(SparkSession spark,
                             com.nimbo.batch.config.HdfsConfig hdfsConfig) {
        super(spark, hdfsConfig.getFullParquetPath());
    }

    @Override
    public void process() {
        logHeader("=== PROTOCOL STATISTICS (Protocol Usage by Hour) ===");

        try {
            Dataset<Row> parquetDf = readParquetFiles();

            Dataset<Row> aggregated = parquetDf
                    .groupBy(
                            col("PROTOCOL").alias("protocol"),
                            window(col("processing_time"), HOUR_WINDOW)
                    )
                    .agg(
                            sum(col("IN_BYTES").plus(col("OUT_BYTES"))).alias("total_bytes"),
                            sum(col("IN_PKTS").plus(col("OUT_PKTS"))).alias("total_packets"),
                            count("*").alias("connection_count")
                    )
                    .withColumn("hour_timestamp", col("window.start"))
                    .drop("window")
                    .orderBy(col("hour_timestamp").desc(), col("total_bytes").desc());

            long recordCount = aggregated.count();
            log.info("Total protocol aggregation records: {}", recordCount);

            if (recordCount == 0) {
                log.warn("No protocol records found in the time window");
                return;
            }

            log.info("Top 20 Protocol Statistics:");
            log.info("Protocol | Hour | Total Bytes | Total Packets | Connection Count");
            log.info("-".repeat(80));

            aggregated.show(20, false);

            log.info("Total protocol records: {}", recordCount);

        } catch (Exception e) {
            log.error("Error processing protocol aggregation", e);
            throw new RuntimeException("Protocol aggregation failed", e);
        }
    }
}