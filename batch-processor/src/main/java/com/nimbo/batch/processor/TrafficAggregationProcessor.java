package com.nimbo.batch.processor;

import com.nimbo.batch.config.BatchConfig;
import com.nimbo.batch.config.HdfsConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.window;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.count;

@Component
@Slf4j
@Order(1)
public class TrafficAggregationProcessor extends BaseProcessor {

    public TrafficAggregationProcessor(SparkSession spark,
                                       HdfsConfig hdfsConfig,
                                       BatchConfig batchConfig) {
        super(spark, hdfsConfig.getFullParquetPath(), batchConfig.getWindowDuration());
    }

    @Override
    public void process() {
        logHeader("=== TRAFFIC AGGREGATION (IP Bandwidth by {} Windows) ===", windowDuration);

        try {
            Dataset<Row> parquetDf = readParquetFiles();

            Dataset<Row> aggregated = parquetDf
                    .groupBy(
                            col("IPV4_SRC_ADDR").alias("src_ip"),
                            window(col("processing_time"), windowDuration)
                    )
                    .agg(
                            sum(col("IN_BYTES")).alias("in_bytes"),
                            sum(col("OUT_BYTES")).alias("out_bytes"),
                            sum(col("IN_PKTS")).alias("in_packets"),
                            sum(col("OUT_PKTS")).alias("out_packets"),
                            count("*").alias("record_count")
                    )
                    .withColumn("window_start", col("window.start"))
                    .withColumn("window_end", col("window.end"))
                    .drop("window")
                    .orderBy(col("window_start").desc(), col("in_bytes").plus(col("out_bytes")).desc());

            long recordCount = aggregated.count();
            log.info("Total aggregation records: {}", recordCount);

            if (recordCount == 0) {
                log.warn("No records found in the time window");
                return;
            }

            log.info("Top 20 Traffic Aggregations (Source IP):");
            log.info("Src IP | Window Start | Window End | In Bytes | Out Bytes | In Packets | Out Packets | Record Count");
            log.info("-".repeat(120));

            aggregated.show(20, false);

            log.info("Total aggregated records: {}", recordCount);

        } catch (Exception e) {
            log.error("Error processing traffic aggregation", e);
            throw new RuntimeException("Traffic aggregation failed", e);
        }
    }
}