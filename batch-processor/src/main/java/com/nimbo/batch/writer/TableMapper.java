package com.nimbo.batch.writer;

import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.Locale;
import java.util.UUID;

@Component
public class TableMapper {

    private static final String BATCH_ID_COLUMN = "batch_id";
    private static final String INGESTED_AT_COLUMN = "ingested_at";

    public void mapRowToStatement(
            PreparedStatement statement,
            Row row,
            TableMapping mapping,
            UUID batchId,
            Timestamp ingestedAt) throws SQLException {

        String[] columns = mapping.getColumnNames();
        String[] columnTypes = mapping.getColumnTypes();

        for (int i = 0; i < columns.length; i++) {
            int parameterIndex = i + 1;
            String column = columns[i];
            String type = columnTypes[i];

            setParameter(
                    statement,
                    parameterIndex,
                    column,
                    type,
                    row,
                    batchId,
                    ingestedAt
            );
        }
    }

    private void setParameter(
            PreparedStatement statement,
            int parameterIndex,
            String column,
            String type,
            Row row,
            UUID batchId,
            Timestamp ingestedAt) throws SQLException {

        if (BATCH_ID_COLUMN.equals(column)) {
            statement.setObject(parameterIndex, batchId);
            return;
        }

        if (INGESTED_AT_COLUMN.equals(column)) {
            statement.setTimestamp(parameterIndex, ingestedAt);
            return;
        }

        Object value = row.getAs(column);

        if (value == null) {
            statement.setNull(parameterIndex, resolveSqlType(type));
            return;
        }

        setTypedValue(statement, parameterIndex, value, type);
    }

    private void setTypedValue(
            PreparedStatement statement,
            int parameterIndex,
            Object value,
            String type) throws SQLException {

        switch (normalizeType(type)) {
            case "string", "varchar", "text" ->
                    statement.setString(parameterIndex, value.toString());

            case "integer", "int" ->
                    statement.setInt(parameterIndex, ((Number) value).intValue());

            case "bigint", "long" ->
                    statement.setLong(parameterIndex, ((Number) value).longValue());

            case "double", "float" ->
                    statement.setDouble(parameterIndex, ((Number) value).doubleValue());

            case "timestamp" ->
                    statement.setTimestamp(parameterIndex, toTimestamp(value));

            case "uuid" ->
                    statement.setObject(parameterIndex, toUuid(value));

            default ->
                    statement.setObject(parameterIndex, value);
        }
    }

    private int resolveSqlType(String type) {
        return switch (normalizeType(type)) {
            case "string", "varchar", "text" -> Types.VARCHAR;
            case "integer", "int" -> Types.INTEGER;
            case "bigint", "long" -> Types.BIGINT;
            case "double", "float" -> Types.DOUBLE;
            case "timestamp" -> Types.TIMESTAMP;
            case "uuid" -> Types.OTHER;
            default -> Types.NULL;
        };
    }

    private String normalizeType(String type) {
        return type.toLowerCase(Locale.ROOT);
    }

    private Timestamp toTimestamp(Object value) {
        if (value instanceof Timestamp timestamp) {
            return timestamp;
        }

        if (value instanceof LocalDateTime localDateTime) {
            return Timestamp.valueOf(localDateTime);
        }

        return Timestamp.valueOf(value.toString());
    }

    private UUID toUuid(Object value) {
        if (value instanceof UUID uuid) {
            return uuid;
        }

        return UUID.fromString(value.toString());
    }
}