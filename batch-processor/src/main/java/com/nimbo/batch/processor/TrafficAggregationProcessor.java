package com.nimbo.batch.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;

import com.nimbo.batch.config.HdfsConfig;

import static org.apache.spark.sql.functions.*;

@Component
@Slf4j
public class TrafficAggregationProcessor extends BaseProcessor {

    private static final String HOUR_WINDOW = "1 hour";

    public TrafficAggregationProcessor(SparkSession spark, HdfsConfig hdfsConfig) {
        super(spark, hdfsConfig.getFullParquetPath());
    }

    @Override
    public void process() {
        logHeader("=== TRAFFIC AGGREGATION (IP Bandwidth by Hour) ===");

        try {
            Dataset<Row> parquetDf = readParquetFiles();

            Dataset<Row> aggregated = parquetDf
                    .groupBy(
                            col("IPV4_SRC_ADDR").alias("ip"),
                            window(col("processing_time"), HOUR_WINDOW)
                    )
                    .agg(
                            sum(col("IN_BYTES").plus(col("OUT_BYTES"))).alias("total_bytes"),
                            sum(col("IN_PKTS").plus(col("OUT_PKTS"))).alias("total_packets"),
                            count("*").alias("record_count")
                    )
                    .withColumn("hour_timestamp", col("window.start"))
                    .drop("window")
                    .orderBy(col("hour_timestamp").desc(), col("total_bytes").desc());

            long recordCount = aggregated.count();
            log.info("Total aggregation records: {}", recordCount);

            if (recordCount == 0) {
                log.warn("No records found in the time window");
                return;
            }

            log.info("Top 20 Traffic Aggregations (Source IP):");
            log.info("IP | Hour | Total Bytes | Total Packets | Record Count");
            log.info("-".repeat(80));

            aggregated.show(20, false);

            log.info("Total aggregated records: {}", recordCount);

        } catch (Exception e) {
            log.error("Error processing traffic aggregation", e);
            throw new RuntimeException("Traffic aggregation failed", e);
        }
    }
}