package com.nimbo.analyticsquery.sql.execution;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbo.analyticsquery.config.SqlExecutionProperties;
import com.nimbo.analyticsquery.model.AnalysisDataSource;
import com.nimbo.analyticsquery.model.AnalyticsResult;
import com.nimbo.analyticsquery.sql.SqlPolicyRules;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class JdbcSqlExecutor implements SqlExecutor {

  private final AnalysisDataSource analysisDataSource;
  private final DataSource dataSource;
  private final ObjectMapper objectMapper;
  private final SqlExecutionProperties properties;

  protected JdbcSqlExecutor(
      AnalysisDataSource analysisDataSource,
      DataSource dataSource,
      ObjectMapper objectMapper,
      SqlExecutionProperties properties) {

    this.analysisDataSource =
        Objects.requireNonNull(analysisDataSource, "analysisDataSource must not be null");

    this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");

    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");

    this.properties = Objects.requireNonNull(properties, "sqlExecutionProperties must not be null");
  }

  @Override
  public final AnalysisDataSource dataSource() {
    return analysisDataSource;
  }

  @Override
  public final AnalyticsResult execute(String sql) {

    if (sql == null || sql.isBlank()) {
      throw new IllegalArgumentException("sql must not be blank");
    }

    try (Connection connection = dataSource.getConnection()) {

      configureConnection(connection);

      try (PreparedStatement statement = connection.prepareStatement(sql)) {

        configureStatement(statement);

        try {
          return executeQuery(statement);

        } catch (SQLException exception) {

          if (isTimeout(exception)) {
            cancelQuietly(statement);

            throw new SqlExecutionException(
                SqlExecutionException.Kind.TIMEOUT, "SQL execution timed out", exception);
          }

          throw exception;
        }
      }

    } catch (SqlExecutionException exception) {
      throw exception;

    } catch (SQLException exception) {
      throw new SqlExecutionException(
          SqlExecutionException.Kind.DATASOURCE_ERROR, "SQL execution failed", exception);
    }
  }

  protected void configureConnection(Connection connection) throws SQLException {

    connection.setReadOnly(true);
  }

  private void configureStatement(PreparedStatement statement) throws SQLException {

    statement.setMaxRows(SqlPolicyRules.maxResultRows());

    statement.setQueryTimeout(queryTimeoutSeconds());
  }

  private AnalyticsResult executeQuery(PreparedStatement statement) throws SQLException {

    try (ResultSet resultSet = statement.executeQuery()) {

      ResultSetMetaData metadata = resultSet.getMetaData();

      List<String> columns = readColumns(metadata);

      List<Map<String, Object>> rows = new ArrayList<>();

      while (resultSet.next()) {

        Map<String, Object> row = readRow(resultSet, metadata);

        rows.add(row);
      }

      AnalyticsResult result = AnalyticsResult.generatedSql(analysisDataSource, columns, rows);

      validateSerializedSize(result);

      return result;
    }
  }

  private List<String> readColumns(ResultSetMetaData metadata) throws SQLException {

    int columnCount = metadata.getColumnCount();

    List<String> columns = new ArrayList<>(columnCount);

    for (int index = 1; index <= columnCount; index++) {

      columns.add(metadata.getColumnLabel(index));
    }

    return List.copyOf(columns);
  }

  private Map<String, Object> readRow(ResultSet resultSet, ResultSetMetaData metadata)
      throws SQLException {

    int columnCount = metadata.getColumnCount();

    Map<String, Object> row = new LinkedHashMap<>(columnCount);

    for (int index = 1; index <= columnCount; index++) {

      String column = metadata.getColumnLabel(index);

      row.put(column, resultSet.getObject(index));
    }

    return row;
  }

  private void validateSerializedSize(AnalyticsResult result) {

    try {
      byte[] serialized = objectMapper.writeValueAsBytes(result);

      if (serialized.length > properties.maxResultSize().toBytes()) {

        throw new SqlExecutionException(
            SqlExecutionException.Kind.RESULT_TOO_LARGE,
            "SQL result exceeded the maximum allowed size");
      }

    } catch (JsonProcessingException exception) {
      throw new SqlExecutionException(
          SqlExecutionException.Kind.RESULT_SERIALIZATION_FAILED,
          "SQL result could not be serialized safely",
          exception);
    }
  }

  private int queryTimeoutSeconds() {

    long milliseconds = properties.statementTimeout().toMillis();

    long seconds = Math.max(1, (milliseconds + 999) / 1000);

    return Math.toIntExact(seconds);
  }

  private boolean isTimeout(SQLException exception) {

    if (exception instanceof SQLTimeoutException) {
      return true;
    }

    String sqlState = exception.getSQLState();

    return "57014".equals(sqlState) || "HYT00".equals(sqlState) || "HYT01".equals(sqlState);
  }

  private void cancelQuietly(PreparedStatement statement) {

    try {
      statement.cancel();

    } catch (SQLException exception) {
      log.warn("Failed to cancel {} SQL statement during cleanup", analysisDataSource, exception);
    }
  }
}
