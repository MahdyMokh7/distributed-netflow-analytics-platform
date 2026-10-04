package com.nimbo.backfill.repository;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.nimbo.backfill.model.BackfillMetrics;
import com.nimbo.backfill.model.SchemaColumn;

final class StarRocksJdbcClient {

    private final Connection connection;

    StarRocksJdbcClient(Connection connection) {
        this.connection = connection;
    }

    List<SchemaColumn> readSchema(String sql) throws SQLException {
        List<SchemaColumn> columns = new ArrayList<>();
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            while (result.next()) {
                columns.add(new SchemaColumn(result.getString("Field"), result.getString("Type"),
                        "YES".equalsIgnoreCase(result.getString("Null"))));
            }
        }
        if (columns.isEmpty()) {
            throw new SQLException("Schema query returned no columns");
        }
        return columns;
    }

    BackfillMetrics queryMetrics(String sql) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            if (!result.next()) {
                throw new SQLException("Metrics query returned no row");
            }
            return new BackfillMetrics(result.getLong("row_count"), decimal(result, "in_bytes"),
                    decimal(result, "out_bytes"), decimal(result, "in_packets"), decimal(result, "out_packets"),
                    nullableLong(result, "minimum_timestamp"), nullableLong(result, "maximum_timestamp"));
        }
    }

    boolean hasRow(String sql) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet result = statement.executeQuery(sql)) {
            return result.next();
        }
    }

    void execute(String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private BigDecimal decimal(ResultSet result, String column) throws SQLException {
        BigDecimal value = result.getBigDecimal(column);
        return value == null ? BigDecimal.ZERO : value;
    }

    private Long nullableLong(ResultSet result, String column) throws SQLException {
        long value = result.getLong(column);
        return result.wasNull() ? null : value;
    }
}
