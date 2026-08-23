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
import static org.apache.spark.sql.functions.count;

@Component
@Slf4j
@Order(4)
public class ProtocolProcessor extends BaseProcessor {

    public ProtocolProcessor(SparkSession spark,
                             HdfsConfig hdfsConfig,
                             BatchConfig batchConfig) {
        super(spark, hdfsConfig.getFullParquetPath(), batchConfig.getWindowDuration());
    }

    @Override
    public void process() {
        logHeader("=== PROTOCOL STATISTICS (Protocol Usage {} Windows) ===", windowDuration);
        try {
            Dataset<Row> parquetDf = readParquetFiles();

            Dataset<Row> aggregated = parquetDf
                    .groupBy(
                            col("L7_PROTO").alias("l7_proto"),
                            window(col("processing_time"), windowDuration)
                    )
                    .agg(
                            sum(col("IN_BYTES")).alias("in_bytes"),
                            sum(col("OUT_BYTES")).alias("out_bytes"),
                            sum(col("IN_PKTS")).alias("in_packets"),
                            sum(col("OUT_PKTS")).alias("out_packets"),
                            count("*").alias("connection_count")
                    )
                    .withColumn("window_start", col("window.start"))
                    .withColumn("window_end", col("window.end"))
                    .drop("window")
                    .orderBy(col("window_start").desc(), col("connection_count").desc());

            long recordCount = aggregated.count();
            log.info("Total protocol aggregation records: {}", recordCount);

            if (recordCount == 0) {
                log.warn("No protocol records found in the time window");
                return;
            }

            log.info("Top 20 Protocol Statistics (by usage):");
            log.info("L7 Proto | Window Start | Window End | In Bytes | Out Bytes | In Packets | Out Packets | Connection Count");
            log.info("-".repeat(140));

            aggregated.show(20, false);
            log.info("Total protocol records: {}", recordCount);

        } catch (Exception e) {
            log.error("Error processing protocol aggregation", e);
            throw new RuntimeException("Protocol aggregation failed", e);
        }
    }
}