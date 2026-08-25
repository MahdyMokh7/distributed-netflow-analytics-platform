package com.nimbo.batch.processor;

import com.nimbo.batch.config.BatchConfig;
import com.nimbo.batch.config.HdfsConfig;
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
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.window;

@Component
@Slf4j
@Order(1)
public class TrafficAggregationProcessor extends BaseProcessor {

    private static final int DISPLAY_LIMIT = 20;

    private final OutputConfig outputConfig;

    public TrafficAggregationProcessor(
            SparkSession spark,
            HdfsConfig hdfsConfig,
            BatchConfig batchConfig,
            OutputConfig outputConfig,
            PostgresWriter postgresWriter) {

        super(
                spark,
                hdfsConfig.getFullParquetPath(),
                batchConfig.getWindowDuration(),
                postgresWriter
        );

        this.outputConfig = outputConfig;
    }

    @Override
    public void process(UUID batchId) {
        logHeader("=== TRAFFIC AGGREGATION (IP Bandwidth by {} Windows) ===", windowDuration);

        try {
            Dataset<Row> aggregated = aggregateTraffic();

            long recordCount = aggregated.count();
            log.info("Total aggregation records: {}", recordCount);

            if (recordCount == 0) {
                logNoRecords();
                return;
            }

            writeOutput(aggregated, batchId);

            log.info("Total aggregated records: {}", recordCount);

        } catch (Exception e) {
            log.error("Error processing traffic aggregation", e);
            throw new RuntimeException("Traffic aggregation failed", e);
        }
    }

    private Dataset<Row> aggregateTraffic() {
        Dataset<Row> parquetDf = readParquetFiles();

        return parquetDf
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
                .orderBy(
                        col("window_start").desc(),
                        col("in_bytes")
                                .plus(col("out_bytes"))
                                .desc()
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
        log.info("Top 20 Traffic Aggregations (Source IP):");
        log.info("Src IP | Window Start | Window End | In Bytes | Out Bytes | In Packets | Out Packets | Record Count");
        log.info("-".repeat(120));

        aggregated.show(DISPLAY_LIMIT, false);
    }

    private void writeToDatabase(Dataset<Row> aggregated, UUID batchId) {
        TableMapping mapping = TableMapping.forTrafficAggregates();

        long written = writeToDatabase(aggregated, mapping, batchId);

        log.info("Written {} records to traffic_aggregates with batch_id: {}", written, batchId);
    }

    private void logNoRecords() {
        log.warn("No records found in the time window");
    }
}