package com.nimbo.batch.writer;

import com.nimbo.batch.config.PostgresConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;

@Component
@Slf4j
public class PostgresWriter {

    private final DataSource dataSource;
    private final int batchSize;
    private final int maxRetryAttempts;
    private final long initialRetryDelayMs;
    private final TableMapper tableMapper;

    public PostgresWriter(
            DataSource postgresDataSource,
            PostgresConfig postgresConfig,
            TableMapper tableMapper) {

        this.dataSource = postgresDataSource;
        this.batchSize = postgresConfig.getBatchSize();
        this.maxRetryAttempts = postgresConfig.getRetry().getMaxAttempts();
        this.initialRetryDelayMs = postgresConfig.getRetry().getInitialDelayMs();
        this.tableMapper = tableMapper;

        log.info(
                "PostgresWriter initialized - batchSize: {}, maxRetries: {}, retryDelayMs: {}",
                batchSize,
                maxRetryAttempts,
                initialRetryDelayMs
        );
    }

    public long write(Dataset<Row> dataset, TableMapping mapping, UUID batchId) {
        Timestamp ingestedAt = Timestamp.from(Instant.now());
        Iterator<Row> rows = dataset.toLocalIterator();

        if (!rows.hasNext()) {
            log.info("No records to write to table: {}", mapping.getTableName());
            return 0;
        }

        String upsertSql = buildUpsertSql(mapping);

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);

            return writeRows(connection, rows, upsertSql, mapping, batchId, ingestedAt);

        } catch (SQLException e) {
            log.error("Database write failed for table: {}, batch_id: {}", mapping.getTableName(), batchId, e);
            throw new RuntimeException("Database write failed for table: " + mapping.getTableName(), e);
        }
    }

    private long writeRows(
            Connection connection,
            Iterator<Row> rows,
            String upsertSql,
            TableMapping mapping,
            UUID batchId,
            Timestamp ingestedAt) throws SQLException {

        long totalWritten = 0;

        try (PreparedStatement statement = connection.prepareStatement(upsertSql)) {
            int currentBatchSize = 0;

            while (rows.hasNext()) {
                tableMapper.mapRowToStatement(statement, rows.next(), mapping, batchId, ingestedAt);

                statement.addBatch();
                currentBatchSize++;

                if (currentBatchSize >= batchSize) {
                    executeBatchWithRetry(statement, connection, mapping.getTableName());

                    totalWritten += currentBatchSize;
                    currentBatchSize = 0;
                }
            }

            if (currentBatchSize > 0) {
                executeBatchWithRetry(statement, connection, mapping.getTableName());
                totalWritten += currentBatchSize;
            }
        }

        log.info("Wrote {} records to table: {} with batch_id: {}", totalWritten, mapping.getTableName(), batchId);

        return totalWritten;
    }

    private void executeBatchWithRetry(
            PreparedStatement statement,
            Connection connection,
            String tableName) throws SQLException {

        int attempts = 0;
        SQLException lastException = null;

        while (attempts < maxRetryAttempts) {
            try {
                statement.executeBatch();
                connection.commit();
                return;
            } catch (SQLException e) {
                lastException = e;
                attempts++;

                connection.rollback();

                if (attempts < maxRetryAttempts) {
                    long delay = calculateRetryDelay(attempts);

                    log.warn(
                            "Batch execution failed for table: {} (attempt {}/{}), retrying in {}ms",
                            tableName,
                            attempts,
                            maxRetryAttempts,
                            delay
                    );

                    sleep(delay);
                }
            }
        }

        throw new SQLException(
                "Batch execution failed after " + maxRetryAttempts + " attempts for table: " + tableName,
                lastException
        );
    }

    private long calculateRetryDelay(int attempt) {
        return initialRetryDelayMs * (long) Math.pow(2.0, (double) attempt - 1);
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Retry backoff interrupted", e);
        }
    }

    private String buildUpsertSql(TableMapping mapping) {
        String[] columns = mapping.getColumnNames();

        String columnList = String.join(", ", columns);
        String placeholders = String.join(", ", Collections.nCopies(columns.length, "?"));
        String conflictKeys = String.join(", ", mapping.getConflictKeyColumns());
        String updateClause = buildUpdateClause(mapping);

        return "INSERT INTO " + mapping.getTableName()
                + " (" + columnList + ")"
                + " VALUES (" + placeholders + ")"
                + " ON CONFLICT (" + conflictKeys + ")"
                + " DO UPDATE SET " + updateClause;
    }

    private String buildUpdateClause(TableMapping mapping) {
        StringBuilder updateClause = new StringBuilder();

        for (String column : mapping.getColumnNames()) {
            if (contains(mapping.getConflictKeyColumns(), column)) {
                continue;
            }

            appendUpdateColumn(updateClause, column);
        }

        return updateClause.toString();
    }

    private void appendUpdateColumn(StringBuilder updateClause, String column) {
        if (!updateClause.isEmpty()) {
            updateClause.append(", ");
        }

        updateClause.append(column).append(" = EXCLUDED.").append(column);
    }

    private boolean contains(String[] array, String value) {
        for (String item : array) {
            if (item.equals(value)) {
                return true;
            }
        }

        return false;
    }
}