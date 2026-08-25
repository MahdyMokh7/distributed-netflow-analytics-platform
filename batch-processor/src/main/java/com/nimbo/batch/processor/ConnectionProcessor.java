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
import static org.apache.spark.sql.functions.sha2;
import static org.apache.spark.sql.functions.concat_ws;

@Component
@Slf4j
@Order(2)
public class ConnectionProcessor extends BaseProcessor {

    private static final int DISPLAY_LIMIT = 20;

    private final OutputConfig outputConfig;

    public ConnectionProcessor(SparkSession spark,
                               HdfsConfig hdfsConfig,
                               BatchConfig batchConfig,
                               OutputConfig outputConfig,
                               PostgresWriter postgresWriter) {
        super(spark, hdfsConfig.getFullParquetPath(), batchConfig.getWindowDuration(), postgresWriter);
        this.outputConfig = outputConfig;
    }

    @Override
    public void process(UUID batchId) {
        logHeader("=== CONNECTION AGGREGATION (IP Connections {} Windows) ===", windowDuration);

        try {
            Dataset<Row> aggregated = aggregateConnections();

            long recordCount = aggregated.count();
            log.info("Total connection aggregation records: {}", recordCount);

            if (recordCount == 0) {
                log.warn("No connection records found in the time window");
                return;
            }

            writeOutput(aggregated, batchId);

            log.info("Total connection records: {}", recordCount);

        } catch (Exception e) {
            log.error("Error processing connection aggregation", e);
            throw new RuntimeException("Connection aggregation failed", e);
        }
    }

    private Dataset<Row> aggregateConnections() {
        Dataset<Row> parquetDf = readParquetFiles();

        return parquetDf
                .groupBy(
                        col("IPV4_SRC_ADDR").alias("src_ip"),
                        col("IPV4_DST_ADDR").alias("dst_ip"),
                        col("L4_SRC_PORT").alias("src_port"),
                        col("L4_DST_PORT").alias("dst_port"),
                        col("PROTOCOL").alias("protocol"),
                        window(col("processing_time"), windowDuration)
                )
                .agg(
                        sum(col("IN_BYTES")).alias("in_bytes"),
                        sum(col("OUT_BYTES")).alias("out_bytes"),
                        sum(col("IN_PKTS")).alias("in_packets"),
                        sum(col("OUT_PKTS")).alias("out_packets")
                )
                .withColumn("window_start", col("window.start"))
                .withColumn("window_end", col("window.end"))
                .drop("window")
                .withColumn("id", sha2(concat_ws("|",
                        col("src_ip"),
                        col("dst_ip"),
                        col("src_port"),
                        col("dst_port"),
                        col("protocol"),
                        col("window_start")
                ), 256))
                .orderBy(col("in_bytes").plus(col("out_bytes")).desc());
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
        log.info("Top 20 Connection Aggregations ({} windows):", windowDuration);
        log.info("Id | Src IP | Dst IP | Src Port | Dst Port | Protocol | Window Start " +
                "| Window End | In Bytes | Out Bytes | In Packets | Out Packets");
        log.info("-".repeat(150));

        aggregated.show(DISPLAY_LIMIT, false);
    }

    private void writeToDatabase(Dataset<Row> aggregated, UUID batchId) {
        TableMapping mapping = TableMapping.forConnectionAggregates();

        long written = writeToDatabase(aggregated, mapping, batchId);

        log.info("Written {} records to connection_aggregates with batch_id: {}", written, batchId);
    }
}