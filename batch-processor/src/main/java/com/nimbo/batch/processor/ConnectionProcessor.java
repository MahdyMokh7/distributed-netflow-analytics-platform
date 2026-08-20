package com.nimbo.batch.processor;

import com.nimbo.batch.config.HdfsConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.*;

@Component
@Slf4j
public class ConnectionProcessor extends BaseProcessor {

    private static final String WINDOW_DURATION = "5 minutes";

    public ConnectionProcessor(SparkSession spark,
                               HdfsConfig hdfsConfig) {
        super(spark, hdfsConfig.getFullParquetPath());
    }

    @Override
    public void process() {
        logHeader("=== CONNECTION AGGREGATION (IP Connections 5-Minute Windows) ===");

        try {
            Dataset<Row> parquetDf = readParquetFiles();

            Dataset<Row> aggregated = parquetDf
                    .groupBy(
                            col("IPV4_SRC_ADDR").alias("src_ip"),
                            col("IPV4_DST_ADDR").alias("dst_ip"),
                            col("L4_SRC_PORT").alias("src_port"),
                            col("L4_DST_PORT").alias("dst_port"),
                            col("PROTOCOL").alias("protocol"),
                            window(col("processing_time"), WINDOW_DURATION)
                    )
                    .agg(
                            sum(col("IN_BYTES").plus(col("OUT_BYTES"))).alias("bytes"),
                            sum(col("IN_PKTS").plus(col("OUT_PKTS"))).alias("packets")
                    )
                    .withColumn("window_start", col("window.start"))
                    .withColumn("window_end", col("window.end"))
                    .drop("window")
                    .orderBy(col("bytes").desc());

            long recordCount = aggregated.count();
            log.info("Total connection aggregation records: {}", recordCount);

            if (recordCount == 0) {
                log.warn("No connection records found in the time window");
                return;
            }

            log.info("Top 20 Connection Aggregations (5-minute windows):");
            log.info("Src IP | Dst IP | Src Port | Dst Port | Protocol | Window Start | Window End | Bytes | Packets");
            log.info("-".repeat(120));

            aggregated.show(20, false);

            log.info("Total connection records: {}", recordCount);

        } catch (Exception e) {
            log.error("Error processing connection aggregation", e);
            throw new RuntimeException("Connection aggregation failed", e);
        }
    }
}