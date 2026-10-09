package com.nimbo.cubemaker.processor;

import static com.nimbo.cubemaker.constants.ColumnConstants.NETFLOW_IN_BYTES;
import static com.nimbo.cubemaker.constants.ColumnConstants.NETFLOW_IN_PKTS;
import static com.nimbo.cubemaker.constants.ColumnConstants.NETFLOW_OUT_BYTES;
import static com.nimbo.cubemaker.constants.ColumnConstants.NETFLOW_OUT_PKTS;
import static com.nimbo.cubemaker.constants.ColumnConstants.NETFLOW_PROCESS_TIME;
import static com.nimbo.cubemaker.constants.ColumnConstants.NETFLOW_SRC_IP;
import static com.nimbo.cubemaker.constants.ColumnConstants.SPARK_IN_BYTES;
import static com.nimbo.cubemaker.constants.ColumnConstants.SPARK_IN_PACKETS;
import static com.nimbo.cubemaker.constants.ColumnConstants.SPARK_OUT_BYTES;
import static com.nimbo.cubemaker.constants.ColumnConstants.SPARK_OUT_PACKETS;
import static com.nimbo.cubemaker.constants.ColumnConstants.SPARK_RECORD_COUNT;
import static com.nimbo.cubemaker.constants.ColumnConstants.SPARK_SRC_IP;
import static com.nimbo.cubemaker.constants.ColumnConstants.SPARK_WINDOW_END;
import static com.nimbo.cubemaker.constants.ColumnConstants.SPARK_WINDOW_START;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.timestamp_millis;
import static org.apache.spark.sql.functions.window;

import com.nimbo.cubemaker.config.BatchConfig;
import com.nimbo.cubemaker.config.OutputConfig;
import com.nimbo.cubemaker.exception.BatchProcessingException;
import com.nimbo.cubemaker.metrics.CubeMakerMetrics.Stage;
import com.nimbo.cubemaker.spark.SparkJobContext;
import com.nimbo.cubemaker.writer.PostgresWriter;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@Order(1)
public class TrafficAggregationProcessor extends BaseProcessor {

  private static final int DISPLAY_LIMIT = 20;

  private final OutputConfig outputConfig;

  public TrafficAggregationProcessor(
      BatchConfig batchConfig, OutputConfig outputConfig, PostgresWriter postgresWriter) {

    super(batchConfig.getWindowDuration(), postgresWriter);

    this.outputConfig = outputConfig;
  }

  @Override
  public Stage metricStage() {
    return Stage.TRAFFIC;
  }

  @Override
  public void process(Dataset<Row> sourceDf, UUID batchId) {
    logHeader("=== TRAFFIC AGGREGATION (IP Bandwidth by {} Windows) ===", windowDuration);

    try {
      Dataset<Row> aggregated = aggregateTraffic(sourceDf);

      long recordCount =
          SparkJobContext.execute(
              sourceDf.sparkSession(),
              batchId,
              "traffic-count",
              "Count traffic aggregate rows",
              aggregated::count);
      log.info("Total aggregation records: {}", recordCount);

      if (recordCount == 0) {
        logNoRecords();
        return;
      }

      writeOutput(aggregated, batchId);

    } catch (Exception e) {
      log.error("Error processing traffic aggregation", e);

      throw new BatchProcessingException(batchId, "Traffic aggregation failed", e);
    }
  }

  private Dataset<Row> aggregateTraffic(Dataset<Row> sourceDf) {
    return sourceDf
        .groupBy(
            col(NETFLOW_SRC_IP).alias(SPARK_SRC_IP),
            window(timestamp_millis(col(NETFLOW_PROCESS_TIME)), windowDuration))
        .agg(
            sum(col(NETFLOW_IN_BYTES)).alias(SPARK_IN_BYTES),
            sum(col(NETFLOW_OUT_BYTES)).alias(SPARK_OUT_BYTES),
            sum(col(NETFLOW_IN_PKTS)).alias(SPARK_IN_PACKETS),
            sum(col(NETFLOW_OUT_PKTS)).alias(SPARK_OUT_PACKETS),
            count("*").alias(SPARK_RECORD_COUNT))
        .withColumn(SPARK_WINDOW_START, col("window.start"))
        .withColumn(SPARK_WINDOW_END, col("window.end"))
        .drop("window");
  }

  private void writeOutput(Dataset<Row> aggregated, UUID batchId) {
    if (outputConfig.isConsoleMode()) {
      writeToConsole(aggregated, batchId);
    }

    if (outputConfig.isDatabaseMode()) {
      writeToDatabase(aggregated, batchId);
    }
  }

  private void writeToConsole(Dataset<Row> aggregated, UUID batchId) {
    log.info("Top 20 Traffic Aggregations in Console (Source IP):");
    log.info(
        "Src IP | Window Start | Window End | In Bytes | Out Bytes | In Packets | Out Packets |"
            + " Record Count");
    log.info("-".repeat(140));

    SparkJobContext.execute(
        aggregated.sparkSession(),
        batchId,
        "traffic-preview",
        "Preview traffic aggregate rows",
        () -> aggregated.show(DISPLAY_LIMIT, false));
  }

  private void writeToDatabase(Dataset<Row> aggregated, UUID batchId) {
    long written = postgresWriter.writeTrafficAggregates(aggregated, batchId);
    log.info("Written {} records to traffic_aggregates with batch_id: {}", written, batchId);
  }

  private void logNoRecords() {
    log.warn("No records found in the time window");
  }
}
