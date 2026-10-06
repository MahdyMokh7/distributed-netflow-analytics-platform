package com.nimbo.cubemaker.writer;

import static com.nimbo.cubemaker.constants.ColumnConstants.COL_DST_IP;
import static com.nimbo.cubemaker.constants.ColumnConstants.COL_ID;
import static com.nimbo.cubemaker.constants.ColumnConstants.COL_INBOUND_BYTES;
import static com.nimbo.cubemaker.constants.ColumnConstants.COL_INBOUND_PACKETS;
import static com.nimbo.cubemaker.constants.ColumnConstants.COL_IN_BYTES;
import static com.nimbo.cubemaker.constants.ColumnConstants.COL_IN_PACKETS;
import static com.nimbo.cubemaker.constants.ColumnConstants.COL_IP;
import static com.nimbo.cubemaker.constants.ColumnConstants.COL_OUTBOUND_BYTES;
import static com.nimbo.cubemaker.constants.ColumnConstants.COL_OUTBOUND_PACKETS;
import static com.nimbo.cubemaker.constants.ColumnConstants.COL_OUT_BYTES;
import static com.nimbo.cubemaker.constants.ColumnConstants.COL_OUT_PACKETS;
import static com.nimbo.cubemaker.constants.ColumnConstants.COL_RECORD_COUNT;
import static com.nimbo.cubemaker.constants.ColumnConstants.COL_SRC_IP;
import static com.nimbo.cubemaker.constants.ColumnConstants.COL_WINDOW_END;
import static com.nimbo.cubemaker.constants.ColumnConstants.COL_WINDOW_START;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HexFormat;
import java.util.UUID;
import org.apache.spark.sql.Row;

enum StagingTable {
  TRAFFIC(
      "traffic_aggregates_staging",
      "DELETE FROM traffic_aggregates_staging WHERE batch_id = ? AND spark_partition_id = ?",
      "COPY traffic_aggregates_staging (src_ip, window_start, window_end, in_bytes, out_bytes, "
          + "in_packets, out_packets, record_count, batch_id, ingested_at, spark_partition_id, "
          + "task_attempt_id) FROM STDIN BINARY",
      """
      INSERT INTO traffic_aggregates
          (src_ip, window_start, window_end, in_bytes, out_bytes, in_packets, out_packets,
           record_count, batch_id, ingested_at)
      SELECT src_ip::inet, window_start, window_end, in_bytes, out_bytes, in_packets,
             out_packets, record_count, batch_id, ingested_at
      FROM traffic_aggregates_staging WHERE batch_id = :batchId
      ON CONFLICT (src_ip, window_start, batch_id) DO UPDATE SET
          window_end = EXCLUDED.window_end,
          in_bytes = EXCLUDED.in_bytes,
          out_bytes = EXCLUDED.out_bytes,
          in_packets = EXCLUDED.in_packets,
          out_packets = EXCLUDED.out_packets,
          record_count = EXCLUDED.record_count,
          ingested_at = EXCLUDED.ingested_at
      """,
      12) {
    @Override
    void writeFields(DataOutputStream output, Row row) throws IOException {
      writeText(output, row.getString(row.fieldIndex(COL_SRC_IP)));
      writeTimestamp(output, row.getTimestamp(row.fieldIndex(COL_WINDOW_START)));
      writeTimestamp(output, row.getTimestamp(row.fieldIndex(COL_WINDOW_END)));
      writeLong(output, row.getLong(row.fieldIndex(COL_IN_BYTES)));
      writeLong(output, row.getLong(row.fieldIndex(COL_OUT_BYTES)));
      writeLong(output, row.getLong(row.fieldIndex(COL_IN_PACKETS)));
      writeLong(output, row.getLong(row.fieldIndex(COL_OUT_PACKETS)));
      writeLong(output, row.getLong(row.fieldIndex(COL_RECORD_COUNT)));
    }
  },
  CONNECTION(
      "connection_aggregates_staging",
      "DELETE FROM connection_aggregates_staging WHERE batch_id = ? AND spark_partition_id = ?",
      "COPY connection_aggregates_staging (id, src_ip, dst_ip, window_start, window_end, in_bytes, "
          + "out_bytes, in_packets, out_packets, batch_id, ingested_at, spark_partition_id, "
          + "task_attempt_id) FROM STDIN BINARY",
      """
      INSERT INTO connection_aggregates
          (id, src_ip, dst_ip, window_start, window_end, in_bytes, out_bytes, in_packets,
           out_packets, batch_id, ingested_at)
      SELECT id, src_ip::inet, dst_ip::inet, window_start, window_end, in_bytes, out_bytes,
             in_packets, out_packets, batch_id, ingested_at
      FROM connection_aggregates_staging WHERE batch_id = :batchId
      ON CONFLICT (id, window_start, batch_id) DO UPDATE SET
          src_ip = EXCLUDED.src_ip,
          dst_ip = EXCLUDED.dst_ip,
          window_end = EXCLUDED.window_end,
          in_bytes = EXCLUDED.in_bytes,
          out_bytes = EXCLUDED.out_bytes,
          in_packets = EXCLUDED.in_packets,
          out_packets = EXCLUDED.out_packets,
          ingested_at = EXCLUDED.ingested_at
      """,
      13) {
    @Override
    void writeFields(DataOutputStream output, Row row) throws IOException {
      writeBytes(output, HexFormat.of().parseHex(row.getString(row.fieldIndex(COL_ID))));
      writeText(output, row.getString(row.fieldIndex(COL_SRC_IP)));
      writeText(output, row.getString(row.fieldIndex(COL_DST_IP)));
      writeTimestamp(output, row.getTimestamp(row.fieldIndex(COL_WINDOW_START)));
      writeTimestamp(output, row.getTimestamp(row.fieldIndex(COL_WINDOW_END)));
      writeLong(output, row.getLong(row.fieldIndex(COL_IN_BYTES)));
      writeLong(output, row.getLong(row.fieldIndex(COL_OUT_BYTES)));
      writeLong(output, row.getLong(row.fieldIndex(COL_IN_PACKETS)));
      writeLong(output, row.getLong(row.fieldIndex(COL_OUT_PACKETS)));
    }
  },
  BANDWIDTH(
      "ip_bandwidth_aggregates_staging",
      "DELETE FROM ip_bandwidth_aggregates_staging WHERE batch_id = ? AND spark_partition_id = ?",
      "COPY ip_bandwidth_aggregates_staging (ip, window_start, window_end, inbound_bytes, "
          + "outbound_bytes, inbound_packets, outbound_packets, batch_id, ingested_at, "
          + "spark_partition_id, task_attempt_id) FROM STDIN BINARY",
      """
      INSERT INTO ip_bandwidth_aggregates
          (ip, window_start, window_end, inbound_bytes, outbound_bytes, inbound_packets,
           outbound_packets, batch_id, ingested_at)
      SELECT ip::inet, window_start, window_end, inbound_bytes, outbound_bytes,
             inbound_packets, outbound_packets, batch_id, ingested_at
      FROM ip_bandwidth_aggregates_staging WHERE batch_id = :batchId
      ON CONFLICT (ip, window_start, batch_id) DO UPDATE SET
          window_end = EXCLUDED.window_end,
          inbound_bytes = EXCLUDED.inbound_bytes,
          outbound_bytes = EXCLUDED.outbound_bytes,
          inbound_packets = EXCLUDED.inbound_packets,
          outbound_packets = EXCLUDED.outbound_packets,
          ingested_at = EXCLUDED.ingested_at
      """,
      11) {
    @Override
    void writeFields(DataOutputStream output, Row row) throws IOException {
      writeText(output, row.getString(row.fieldIndex(COL_IP)));
      writeTimestamp(output, row.getTimestamp(row.fieldIndex(COL_WINDOW_START)));
      writeTimestamp(output, row.getTimestamp(row.fieldIndex(COL_WINDOW_END)));
      writeLong(output, row.getLong(row.fieldIndex(COL_INBOUND_BYTES)));
      writeLong(output, row.getLong(row.fieldIndex(COL_OUTBOUND_BYTES)));
      writeLong(output, row.getLong(row.fieldIndex(COL_INBOUND_PACKETS)));
      writeLong(output, row.getLong(row.fieldIndex(COL_OUTBOUND_PACKETS)));
    }
  };

  private static final LocalDateTime POSTGRES_EPOCH = LocalDateTime.of(2000, 1, 1, 0, 0);

  private final String stagingTable;
  private final String deletePartitionAttemptSql;
  private final String copySql;
  private final String mergeSql;
  private final int fieldCount;

  StagingTable(
      String stagingTable,
      String deletePartitionAttemptSql,
      String copySql,
      String mergeSql,
      int fieldCount) {
    this.stagingTable = stagingTable;
    this.deletePartitionAttemptSql = deletePartitionAttemptSql;
    this.copySql = copySql;
    this.mergeSql = mergeSql;
    this.fieldCount = fieldCount;
  }

  String stagingTable() {
    return stagingTable;
  }

  String deletePartitionAttemptSql() {
    return deletePartitionAttemptSql;
  }

  String copySql() {
    return copySql;
  }

  String mergeSql() {
    return mergeSql;
  }

  void writeRow(
      DataOutputStream output,
      Row row,
      UUID batchId,
      Timestamp ingestedAt,
      int partitionId,
      int attemptId)
      throws IOException {
    output.writeShort(fieldCount);
    writeFields(output, row);
    writeUuid(output, batchId);
    writeTimestamp(output, ingestedAt);
    writeInt(output, partitionId);
    writeInt(output, attemptId);
  }

  abstract void writeFields(DataOutputStream output, Row row) throws IOException;

  private static void writeText(DataOutputStream output, String value) throws IOException {
    writeBytes(output, value.getBytes(StandardCharsets.UTF_8));
  }

  private static void writeBytes(DataOutputStream output, byte[] value) throws IOException {
    output.writeInt(value.length);
    output.write(value);
  }

  private static void writeLong(DataOutputStream output, long value) throws IOException {
    output.writeInt(Long.BYTES);
    output.writeLong(value);
  }

  private static void writeInt(DataOutputStream output, int value) throws IOException {
    output.writeInt(Integer.BYTES);
    output.writeInt(value);
  }

  private static void writeTimestamp(DataOutputStream output, Timestamp value) throws IOException {
    writeTimestamp(output, value.toLocalDateTime());
  }

  private static void writeTimestamp(DataOutputStream output, LocalDateTime value)
      throws IOException {
    output.writeInt(Long.BYTES);
    output.writeLong(ChronoUnit.MICROS.between(POSTGRES_EPOCH, value));
  }

  private static void writeUuid(DataOutputStream output, UUID value) throws IOException {
    output.writeInt(16);
    output.writeLong(value.getMostSignificantBits());
    output.writeLong(value.getLeastSignificantBits());
  }
}
