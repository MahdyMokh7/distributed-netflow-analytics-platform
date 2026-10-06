package com.nimbo.cubemaker.processor;

import static com.nimbo.cubemaker.constants.ColumnConstants.NETFLOW_DST_IP;
import static com.nimbo.cubemaker.constants.ColumnConstants.NETFLOW_IN_BYTES;
import static com.nimbo.cubemaker.constants.ColumnConstants.NETFLOW_IN_PKTS;
import static com.nimbo.cubemaker.constants.ColumnConstants.NETFLOW_OUT_BYTES;
import static com.nimbo.cubemaker.constants.ColumnConstants.NETFLOW_OUT_PKTS;
import static com.nimbo.cubemaker.constants.ColumnConstants.NETFLOW_PROCESS_TIME;
import static com.nimbo.cubemaker.constants.ColumnConstants.NETFLOW_SRC_IP;
import static com.nimbo.cubemaker.constants.ColumnConstants.SPARK_INBOUND_BYTES;
import static com.nimbo.cubemaker.constants.ColumnConstants.SPARK_INBOUND_PACKETS;
import static com.nimbo.cubemaker.constants.ColumnConstants.SPARK_IP;
import static com.nimbo.cubemaker.constants.ColumnConstants.SPARK_OUTBOUND_BYTES;
import static com.nimbo.cubemaker.constants.ColumnConstants.SPARK_OUTBOUND_PACKETS;
import static com.nimbo.cubemaker.constants.ColumnConstants.SPARK_WINDOW_END;
import static com.nimbo.cubemaker.constants.ColumnConstants.SPARK_WINDOW_START;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.timestamp_millis;
import static org.apache.spark.sql.functions.window;

import com.nimbo.cubemaker.config.BatchConfig;
import com.nimbo.cubemaker.config.OutputConfig;
import com.nimbo.cubemaker.exception.BatchProcessingException;
import com.nimbo.cubemaker.metrics.CubeMakerMetrics.Stage;
import com.nimbo.cubemaker.writer.PostgresWriter;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@Order(3)
public class TopIpProcessor extends BaseProcessor {

  private static final int TOP_N = 20;

  private final OutputConfig outputConfig;

  public TopIpProcessor(
      BatchConfig batchConfig, OutputConfig outputConfig, PostgresWriter postgresWriter) {

    super(batchConfig.getWindowDuration(), postgresWriter);

    this.outputConfig = outputConfig;
  }

  @Override
  public Stage metricStage() {
    return Stage.TOP_IP;
  }

  @Override
  public void process(Dataset<Row> sourceDf, UUID batchId) {
    logHeader("=== TOP IPs BY BANDWIDTH CONSUMPTION (per {} windows) ===", windowDuration);

    try {
      Dataset<Row> aggregated = aggregateTopIps(sourceDf);

      long recordCount = aggregated.count();
      log.info("Total unique IP/window aggregates: {}", recordCount);

      if (recordCount == 0) {
        log.warn("No IP records found in the time window");
        return;
      }

      writeOutput(aggregated, batchId);

    } catch (RuntimeException e) {
      log.error("Error processing top IP aggregation", e);

      throw new BatchProcessingException(batchId, "Top IP aggregation failed", e);
    }
  }

  private Dataset<Row> aggregateTopIps(Dataset<Row> sourceDf) {
    Dataset<Row> srcPerspective =
        sourceDf.select(
            col(NETFLOW_SRC_IP).alias(SPARK_IP),
            col(NETFLOW_OUT_BYTES).alias(SPARK_OUTBOUND_BYTES),
            col(NETFLOW_IN_BYTES).alias(SPARK_INBOUND_BYTES),
            col(NETFLOW_OUT_PKTS).alias(SPARK_OUTBOUND_PACKETS),
            col(NETFLOW_IN_PKTS).alias(SPARK_INBOUND_PACKETS),
            timestamp_millis(col(NETFLOW_PROCESS_TIME)).alias(NETFLOW_PROCESS_TIME));

    Dataset<Row> dstPerspective =
        sourceDf.select(
            col(NETFLOW_DST_IP).alias(SPARK_IP),
            col(NETFLOW_IN_BYTES).alias(SPARK_OUTBOUND_BYTES),
            col(NETFLOW_OUT_BYTES).alias(SPARK_INBOUND_BYTES),
            col(NETFLOW_IN_PKTS).alias(SPARK_OUTBOUND_PACKETS),
            col(NETFLOW_OUT_PKTS).alias(SPARK_INBOUND_PACKETS),
            timestamp_millis(col(NETFLOW_PROCESS_TIME)).alias(NETFLOW_PROCESS_TIME));

    Dataset<Row> unified = srcPerspective.unionByName(dstPerspective);

    return unified
        .groupBy(col(SPARK_IP), window(col(NETFLOW_PROCESS_TIME), windowDuration))
        .agg(
            sum(col(SPARK_OUTBOUND_BYTES)).alias(SPARK_OUTBOUND_BYTES),
            sum(col(SPARK_INBOUND_BYTES)).alias(SPARK_INBOUND_BYTES),
            sum(col(SPARK_OUTBOUND_PACKETS)).alias(SPARK_OUTBOUND_PACKETS),
            sum(col(SPARK_INBOUND_PACKETS)).alias(SPARK_INBOUND_PACKETS))
        .withColumn(SPARK_WINDOW_START, col("window.start"))
        .withColumn(SPARK_WINDOW_END, col("window.end"))
        .drop("window");
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
    log.info(
        "Top {} IPs by Bandwidth Consumption in Console (per {} windows):", TOP_N, windowDuration);
    log.info(
        "IP | Window Start | Window End | Outbound Bytes | "
            + "Inbound Bytes | Outbound Packets | Inbound Packets");
    log.info("-".repeat(140));

    aggregated.show(TOP_N, false);
  }

  private void writeToDatabase(Dataset<Row> aggregated, UUID batchId) {
    long written = postgresWriter.writeIpBandwidthAggregates(aggregated, batchId);
    log.info("Written {} records to ip_bandwidth_aggregates with batch_id: {}", written, batchId);
  }
}
