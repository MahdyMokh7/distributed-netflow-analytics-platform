package com.nimbo.cubemaker.writer;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serial;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.Row;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyIn;

@Slf4j
final class StagingPartitionWriter implements Serializable {

  @Serial private static final long serialVersionUID = 1L;
  private static final byte[] COPY_HEADER =
      new byte[] {
          'P', 'G', 'C', 'O', 'P', 'Y', '\n', (byte) 0xff, '\r', '\n', 0, 0, 0, 0, 0, 0, 0, 0, 0
      };

  private final StagingTable table;
  private final JdbiDaoFactory daoFactory;
  private final UUID batchId;
  private final Timestamp ingestedAt;

  StagingPartitionWriter(
      StagingTable table, JdbiDaoFactory daoFactory, UUID batchId, Timestamp ingestedAt) {
    this.table = table;
    this.daoFactory = daoFactory;
    this.batchId = batchId;
    this.ingestedAt = ingestedAt;
  }

  void call(Iterator<Row> rows) {
    if (!rows.hasNext()) {
      return;
    }

    TaskContext context = TaskContext.get();
    int partitionId = context == null ? -1 : context.partitionId();
    int attemptId = context == null ? 0 : context.attemptNumber();
    long copiedRows = 0;
    CopyIn copy = null;

    try (Connection connection = daoFactory.openConnection()) {
      connection.setAutoCommit(false);
      try {
        deletePreviousPartitionAttempt(connection, partitionId);
        copy = connection.unwrap(PGConnection.class).getCopyAPI().copyIn(table.copySql());
        try (DataOutputStream output =
            new DataOutputStream(new BufferedOutputStream(new CopyOutputStream(copy), 64 * 1024))) {
          output.write(COPY_HEADER);
          do {
            table.writeRow(output, rows.next(), batchId, ingestedAt, partitionId, attemptId);
            copiedRows++;
          } while (rows.hasNext());
          output.writeShort(-1);
        }
        copy.endCopy();
        copy = null;
        connection.commit();
        log.info(
            "Copied {} rows into {} for Spark partition {} attempt {}",
            copiedRows,
            table.stagingTable(),
            partitionId,
            attemptId);
      } catch (Exception exception) {
        if (copy != null && copy.isActive()) {
          copy.cancelCopy();
        }
        connection.rollback();
        throw exception;
      }
    } catch (Exception exception) {
      throw new IllegalStateException(
          "Failed to stage Spark partition " + partitionId + " in " + table.stagingTable(),
          exception);
    }
  }

  private void deletePreviousPartitionAttempt(Connection connection, int partitionId)
      throws Exception {
    String sql =
        "DELETE FROM " + table.stagingTable() + " WHERE batch_id = ? AND spark_partition_id = ?";
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setObject(1, batchId);
      statement.setInt(2, partitionId);
      statement.executeUpdate();
    }
  }

  private static final class CopyOutputStream extends OutputStream {
    private final CopyIn copy;

    private CopyOutputStream(CopyIn copy) {
      this.copy = copy;
    }

    @Override
    public void write(int value) throws IOException {
      byte[] single = {(byte) value};
      write(single, 0, 1);
    }

    @Override
    public void write(byte[] bytes, int offset, int length) throws IOException {
      try {
        copy.writeToCopy(bytes, offset, length);
      } catch (java.sql.SQLException exception) {
        throw new IOException("PostgreSQL binary COPY failed", exception);
      }
    }
  }
}
