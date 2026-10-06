package com.nimbo.cubemaker.processor;

import static com.nimbo.cubemaker.constants.ColumnConstants.NETFLOW_IN_BYTES;
import static com.nimbo.cubemaker.constants.ColumnConstants.NETFLOW_IN_PKTS;
import static com.nimbo.cubemaker.constants.ColumnConstants.NETFLOW_L7_PROTO;
import static com.nimbo.cubemaker.constants.ColumnConstants.NETFLOW_OUT_BYTES;
import static com.nimbo.cubemaker.constants.ColumnConstants.NETFLOW_OUT_PKTS;
import static com.nimbo.cubemaker.constants.ColumnConstants.NETFLOW_PROCESS_TIME;
import static com.nimbo.cubemaker.constants.ColumnConstants.SPARK_CONNECTION_COUNT;
import static com.nimbo.cubemaker.constants.ColumnConstants.SPARK_IN_BYTES;
import static com.nimbo.cubemaker.constants.ColumnConstants.SPARK_IN_PACKETS;
import static com.nimbo.cubemaker.constants.ColumnConstants.SPARK_L7_PROTOCOL;
import static com.nimbo.cubemaker.constants.ColumnConstants.SPARK_OUT_BYTES;
import static com.nimbo.cubemaker.constants.ColumnConstants.SPARK_OUT_PACKETS;
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
import com.nimbo.cubemaker.writer.PostgresWriter;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@Order(4)
public class ProtocolProcessor extends BaseProcessor {

  private static final int DISPLAY_LIMIT = 20;

  private final OutputConfig outputConfig;

  public ProtocolProcessor(
      BatchConfig batchConfig, OutputConfig outputConfig, PostgresWriter postgresWriter) {

    super(batchConfig.getWindowDuration(), postgresWriter);

    this.outputConfig = outputConfig;
  }

  @Override
  public Stage metricStage() {
    return Stage.PROTOCOL;
  }

  @Override
  public void process(Dataset<Row> sourceDf, UUID batchId) {
    logHeader("=== PROTOCOL STATISTICS (Protocol Usage {} Windows) ===", windowDuration);

    try {
      Dataset<Row> aggregated = aggregateProtocols(sourceDf);

      long recordCount = aggregated.count();
      log.info("Total protocol aggregation records: {}", recordCount);

      if (recordCount == 0) {
        log.warn("No protocol records found in the time window");
        return;
      }

      writeOutput(aggregated, batchId);

    } catch (RuntimeException e) {

      log.error("Error processing protocol aggregation", e);
      throw new BatchProcessingException(batchId, "Protocol aggregation failed", e);
    }
  }

  private Dataset<Row> aggregateProtocols(Dataset<Row> sourceDf) {
    return sourceDf
        .groupBy(
            col(NETFLOW_L7_PROTO).alias(SPARK_L7_PROTOCOL),
            window(timestamp_millis(col(NETFLOW_PROCESS_TIME)), windowDuration))
        .agg(
            sum(col(NETFLOW_IN_BYTES)).alias(SPARK_IN_BYTES),
            sum(col(NETFLOW_OUT_BYTES)).alias(SPARK_OUT_BYTES),
            sum(col(NETFLOW_IN_PKTS)).alias(SPARK_IN_PACKETS),
            sum(col(NETFLOW_OUT_PKTS)).alias(SPARK_OUT_PACKETS),
            count("*").alias(SPARK_CONNECTION_COUNT))
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
    log.info("Top 20 Protocol Statistics in Console (by usage):");
    log.info(
        "L7 Proto | Window Start | Window End | In Bytes | "
            + "Out Bytes | In Packets | Out Packets | Connection Count");
    log.info("-".repeat(140));

    aggregated.show(DISPLAY_LIMIT, false);
  }

  private void writeToDatabase(Dataset<Row> aggregated, UUID batchId) {
    long written = postgresWriter.writeProtocolAggregates(aggregated, batchId);
    log.info("Written {} records to protocol_usage_aggregates with batch_id: {}", written, batchId);
  }
}
