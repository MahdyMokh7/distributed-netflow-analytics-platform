package com.nimbo.starrockswriter.schema;

import com.nimbo.starrockswriter.config.StarRocksProperties;
import com.nimbo.starrockswriter.mapper.SchemaMapping;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class StarRocksSchemaEvolutionManager {

  private final JdbcTemplate jdbcTemplate;
  private final StarRocksProperties properties;
  private final SchemaMapping schemaMapping;

  private volatile List<String> netFlowColumns = List.of();
  private final Set<Long> ensuredSchemas = ConcurrentHashMap.newKeySet();

  public void ensureSchema(Schema writerSchema) {
    long fingerprint = SchemaNormalization.parsingFingerprint64(writerSchema);
    if (ensuredSchemas.contains(fingerprint)) {
      return;
    }

    synchronized (this) {
      if (ensuredSchemas.contains(fingerprint)) {
        return;
      }
      ensureSchemaOnce(writerSchema);
      ensuredSchemas.add(fingerprint);
    }
  }

  private void ensureSchemaOnce(Schema writerSchema) {
    List<TableColumn> tableColumns = loadTableColumns();
    Set<String> existing =
        tableColumns.stream()
            .map(TableColumn::name)
            .map(name -> name.toLowerCase(Locale.ROOT))
            .collect(Collectors.toCollection(HashSet::new));

    List<Schema.Field> missing =
        writerSchema.getFields().stream()
            .filter(field -> !existing.contains(field.name().toLowerCase(Locale.ROOT)))
            .toList();

    if (!missing.isEmpty()) {
      addColumnsAndWait(missing);
      tableColumns = loadTableColumns();
    }

    evolveExistingColumnTypes(writerSchema, tableColumns);
    updateColumnSnapshot(loadTableColumns());
  }

  public List<String> currentNetFlowColumns() {
    List<String> snapshot = netFlowColumns;
    if (!snapshot.isEmpty()) {
      return snapshot;
    }
    synchronized (this) {
      if (netFlowColumns.isEmpty()) {
        updateColumnSnapshot(loadTableColumns());
      }
      return netFlowColumns;
    }
  }

  private void evolveExistingColumnTypes(Schema writerSchema, List<TableColumn> tableColumns) {
    Map<String, TableColumn> byName =
        tableColumns.stream()
            .collect(
                Collectors.toMap(
                    column -> column.name().toLowerCase(Locale.ROOT), Function.identity()));

    for (Schema.Field field : writerSchema.getFields()) {
      String name = field.name().toLowerCase(Locale.ROOT);
      TableColumn column = byName.get(name);
      if (column == null) {
        continue;
      }

      SchemaMapping.TypeEvolution evolution =
          schemaMapping.typeEvolution(column.columnType(), field.schema());
      if (evolution == SchemaMapping.TypeEvolution.COMPATIBLE) {
        continue;
      }

      String desiredType = schemaMapping.starRocksType(field.schema());
      if (evolution == SchemaMapping.TypeEvolution.INCOMPATIBLE) {
        throw new IllegalStateException(
            "Unsafe StarRocks type change for column "
                + name
                + ": existing="
                + column.columnType()
                + ", Avro requires="
                + desiredType
                + ". Automatic schema evolution only performs lossless widening.");
      }

      if (column.isKey()) {
        throw new IllegalStateException(
            "Automatic type widening is not performed for StarRocks key column "
                + name
                + ". Existing="
                + column.columnType()
                + ", requested="
                + desiredType);
      }

      modifyColumnAndWait(field, column);
      byName.put(
          name,
          loadTableColumns().stream()
              .filter(current -> current.name().equalsIgnoreCase(name))
              .findFirst()
              .orElseThrow());
    }
  }

  private void addColumnsAndWait(List<Schema.Field> missing) {
    Set<String> targetColumns =
        missing.stream()
            .map(Schema.Field::name)
            .map(name -> name.toLowerCase(Locale.ROOT))
            .collect(Collectors.toSet());

    long deadline = System.currentTimeMillis() + properties.getSchemaEvolutionTimeoutMs();
    boolean submitted = false;

    while (System.currentTimeMillis() < deadline) {
      Set<String> existing =
          loadTableColumns().stream()
              .map(TableColumn::name)
              .map(name -> name.toLowerCase(Locale.ROOT))
              .collect(Collectors.toSet());

      if (existing.containsAll(targetColumns)) {
        log.info("StarRocks schema evolution complete. Added columns={}", targetColumns);
        return;
      }

      if (!submitted) {
        List<Schema.Field> stillMissing =
            missing.stream()
                .filter(field -> !existing.contains(field.name().toLowerCase(Locale.ROOT)))
                .toList();
        try {
          jdbcTemplate.execute(buildAddColumnsSql(stillMissing));
          submitted = true;
          log.info(
              "Submitted StarRocks ADD COLUMN for fields={}",
              stillMissing.stream().map(Schema.Field::name).toList());
        } catch (CannotGetJdbcConnectionException exception) {
          throw exception;
        } catch (DataAccessException exception) {
          log.warn(
              "StarRocks ADD COLUMN was not accepted yet; will re-check table schema: {}",
              exception.getMostSpecificCause().getMessage());
        }
      }

      sleep(properties.getSchemaEvolutionPollMs());
    }

    throw new IllegalStateException(
        "Timed out waiting for StarRocks schema evolution. Missing columns=" + targetColumns);
  }

  private void modifyColumnAndWait(Schema.Field field, TableColumn originalColumn) {
    String columnName = field.name().toLowerCase(Locale.ROOT);
    String desiredType = schemaMapping.starRocksType(field.schema());
    long deadline = System.currentTimeMillis() + properties.getSchemaEvolutionTimeoutMs();
    boolean submitted = false;

    while (System.currentTimeMillis() < deadline) {
      TableColumn current =
          loadTableColumns().stream()
              .filter(column -> column.name().equalsIgnoreCase(columnName))
              .findFirst()
              .orElseThrow(
                  () -> new IllegalStateException("StarRocks column disappeared: " + columnName));

      if (schemaMapping.typeEvolution(current.columnType(), field.schema())
          == SchemaMapping.TypeEvolution.COMPATIBLE) {
        log.info(
            "StarRocks type evolution complete. Column={} type={}",
            columnName,
            current.columnType());
        return;
      }

      if (!submitted) {
        try {
          jdbcTemplate.execute(buildModifyColumnSql(field, originalColumn));
          submitted = true;
          log.info(
              "Submitted StarRocks MODIFY COLUMN: column={}, from={}, to={}",
              columnName,
              originalColumn.columnType(),
              desiredType);
        } catch (CannotGetJdbcConnectionException exception) {
          throw exception;
        } catch (DataAccessException exception) {
          log.warn(
              "StarRocks MODIFY COLUMN was not accepted yet; will re-check table schema: {}",
              exception.getMostSpecificCause().getMessage());
        }
      }

      sleep(properties.getSchemaEvolutionPollMs());
    }

    throw new IllegalStateException(
        "Timed out waiting for StarRocks type evolution. Column="
            + columnName
            + ", requestedType="
            + desiredType);
  }

  private String buildAddColumnsSql(List<Schema.Field> fields) {
    if (fields.isEmpty()) {
      throw new IllegalArgumentException("No columns to add");
    }

    String definitions =
        fields.stream()
            .map(
                field ->
                    quoteIdentifier(field.name().toLowerCase(Locale.ROOT))
                        + " "
                        + schemaMapping.starRocksType(field.schema())
                        + " NULL"
                        + commentClause(schemaMapping.fieldComment(field)))
            .collect(Collectors.joining(", "));

    return "ALTER TABLE " + qualifiedTable() + " ADD COLUMN (" + definitions + ")";
  }

  private String buildModifyColumnSql(Schema.Field field, TableColumn column) {
    StringBuilder sql =
        new StringBuilder("ALTER TABLE ")
            .append(qualifiedTable())
            .append(" MODIFY COLUMN ")
            .append(quoteIdentifier(column.name()))
            .append(" ")
            .append(schemaMapping.starRocksType(field.schema()));

    if (column.isKey()) {
      sql.append(" KEY");
    }
    sql.append(column.nullable() ? " NULL" : " NOT NULL");

    if (column.defaultValue() != null) {
      sql.append(" DEFAULT \"").append(escapeSqlString(column.defaultValue())).append("\"");
    }

    String comment = schemaMapping.fieldComment(field);
    if (comment.isBlank()) {
      comment = column.comment();
    }
    sql.append(commentClause(comment));
    return sql.toString();
  }

  private String commentClause(String comment) {
    if (comment == null || comment.isBlank()) {
      return "";
    }
    return " COMMENT \"" + escapeSqlString(comment) + "\"";
  }

  private String escapeSqlString(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  private List<TableColumn> loadTableColumns() {
    String sql =
        """
        SELECT COLUMN_NAME,
               ORDINAL_POSITION,
               COLUMN_TYPE,
               IS_NULLABLE,
               COLUMN_KEY,
               COLUMN_DEFAULT,
               COLUMN_COMMENT
        FROM information_schema.columns
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """;

    List<TableColumn> columns =
        jdbcTemplate.query(
            sql,
            (rs, rowNum) ->
                new TableColumn(
                    rs.getString("COLUMN_NAME"),
                    rs.getInt("ORDINAL_POSITION"),
                    rs.getString("COLUMN_TYPE"),
                    "YES".equalsIgnoreCase(rs.getString("IS_NULLABLE")),
                    rs.getString("COLUMN_KEY"),
                    rs.getString("COLUMN_DEFAULT"),
                    rs.getString("COLUMN_COMMENT")),
            properties.getDatabase(),
            properties.getTable());

    if (columns.isEmpty()) {
      throw new IllegalStateException(
          "StarRocks table does not exist: "
              + properties.getDatabase()
              + "."
              + properties.getTable());
    }
    return columns;
  }

  private void updateColumnSnapshot(List<TableColumn> columns) {
    Set<String> kafkaColumns =
        SchemaMapping.KAFKA_COLUMNS.stream()
            .map(column -> column.toLowerCase(Locale.ROOT))
            .collect(Collectors.toCollection(HashSet::new));
    List<String> dynamicColumns = new ArrayList<>();

    for (TableColumn column : columns) {
      String normalized = column.name().toLowerCase(Locale.ROOT);
      if (!kafkaColumns.contains(normalized)) {
        dynamicColumns.add(normalized);
      }
    }
    netFlowColumns = List.copyOf(dynamicColumns);
  }

  private String qualifiedTable() {
    return quoteIdentifier(properties.getDatabase()) + "." + quoteIdentifier(properties.getTable());
  }

  private String quoteIdentifier(String identifier) {
    if (!identifier.matches("[A-Za-z0-9_]+")) {
      throw new IllegalArgumentException("Unsafe SQL identifier: " + identifier);
    }
    return "`" + identifier + "`";
  }

  private void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "Interrupted while waiting for StarRocks schema evolution", exception);
    }
  }

  private record TableColumn(
      String name,
      int ordinalPosition,
      String columnType,
      boolean nullable,
      String columnKey,
      String defaultValue,
      String comment) {
    boolean isKey() {
      return columnKey != null && !columnKey.isBlank();
    }
  }
}
