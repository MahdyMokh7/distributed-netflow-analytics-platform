package com.nimbo.starrockswriter.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nimbo.starrockswriter.config.StarRocksProperties;
import com.nimbo.starrockswriter.mapper.SchemaMapping;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

class StarRocksSchemaEvolutionManagerTest {

  @Test
  void shouldLoadAndCacheCurrentNetFlowColumns() {
    FakeJdbcTemplate jdbc = new FakeJdbcTemplate();
    jdbc.add(baseColumn("kafka_topic", "varchar(255)"));
    jdbc.add(baseColumn("kafka_partition", "int(11)"));
    jdbc.add(baseColumn("kafka_offset", "bigint(20)"));
    jdbc.add(baseColumn("PROCESS_TIME", "bigint(20)"));
    jdbc.add(baseColumn("IPV4_SRC_ADDR", "varchar(15)"));
    jdbc.add(baseColumn("__internal_shadow", "bigint(20)"));
    jdbc.add(baseColumn("IN_PKTS", "bigint(20)"));

    StarRocksSchemaEvolutionManager manager = manager(jdbc, properties());

    assertEquals(List.of("ipv4_src_addr", "in_pkts"), manager.currentNetFlowColumns());
    int queriesAfterFirstLoad = jdbc.queryCount();
    assertEquals(1, jdbc.queries().size());
    assertTrue(jdbc.queries().get(0).isColumnLoadFor("nimbo", "netflow"));

    assertEquals(List.of("ipv4_src_addr", "in_pkts"), manager.currentNetFlowColumns());
    assertEquals(queriesAfterFirstLoad, jdbc.queryCount());
  }

  @Test
  void shouldAddMissingColumnWithMappedTypeAndAvroComment() {
    FakeJdbcTemplate jdbc = new FakeJdbcTemplate();
    addKafkaMetadataColumns(jdbc);
    jdbc.add(baseColumn("existing", "int(11)"));
    jdbc.onExecute(sql -> {
      if (sql.contains("ADD COLUMN")) {
        jdbc.add(new ColumnRow(
            "exporter_name",
            jdbc.nextOrdinal(),
            "varchar(65533)",
            true,
            "",
            null,
            "NetFlow exporter name."));
      }
    });

    Schema schema = schema(
        """
            {
              "type":"record",
              "name":"AddColumnRecord",
              "fields":[
                {"name":"EXISTING","type":"int"},
                {
                  "name":"EXPORTER_NAME",
                  "type":["null","string"],
                  "default":null,
                  "doc":"NetFlow exporter name."
                }
              ]
            }
            """);

    StarRocksSchemaEvolutionManager manager = manager(jdbc, properties());
    manager.ensureSchema(schema);

    assertEquals(1, jdbc.executedSql().size());
    assertEquals(
        "ALTER TABLE `nimbo`.`netflow` ADD COLUMN "
            + "(`exporter_name` VARCHAR(65533) NULL COMMENT \"NetFlow exporter name.\")",
        jdbc.executedSql().get(0));
    assertEquals(List.of("existing", "exporter_name"), manager.currentNetFlowColumns());

    int queryCount = jdbc.queryCount();
    manager.ensureSchema(schema);
    assertEquals(queryCount, jdbc.queryCount());
    assertEquals(1, jdbc.executedSql().size());
  }

  @Test
  void shouldWidenIntToBigintAndPreserveExistingColumnAttributes() {
    FakeJdbcTemplate jdbc = new FakeJdbcTemplate();
    addKafkaMetadataColumns(jdbc);
    jdbc.add(new ColumnRow(
        "counter",
        jdbc.nextOrdinal(),
        "int(11)",
        false,
        "",
        "7",
        "Existing counter comment."));
    jdbc.onExecute(sql -> {
      if (sql.contains("MODIFY COLUMN")) {
        jdbc.replaceType("counter", "bigint(20)");
      }
    });

    Schema schema = schema(
        """
            {
              "type":"record",
              "name":"WidenRecord",
              "fields":[
                {"name":"COUNTER","type":"long"}
              ]
            }
            """);

    manager(jdbc, properties()).ensureSchema(schema);

    assertEquals(1, jdbc.executedSql().size());
    assertEquals(
        "ALTER TABLE `nimbo`.`netflow` MODIFY COLUMN `counter` BIGINT "
            + "NOT NULL DEFAULT \"7\" COMMENT \"Existing counter comment.\"",
        jdbc.executedSql().get(0));
  }

  @Test
  void shouldRejectUnsafeExistingTypeChangeWithoutExecutingDdl() {
    FakeJdbcTemplate jdbc = new FakeJdbcTemplate();
    jdbc.add(baseColumn("counter", "double"));

    Schema schema = schema(
        """
            {
              "type":"record",
              "name":"UnsafeTypeRecord",
              "fields":[{"name":"COUNTER","type":"long"}]
            }
            """);

    IllegalStateException exception = assertThrows(
        IllegalStateException.class,
        () -> manager(jdbc, properties()).ensureSchema(schema));

    assertTrue(exception.getMessage().contains("Unsafe StarRocks type change for column counter"));
    assertTrue(jdbc.executedSql().isEmpty());
  }

  @Test
  void shouldRejectAutomaticWideningForKeyColumn() {
    FakeJdbcTemplate jdbc = new FakeJdbcTemplate();
    jdbc.add(new ColumnRow("flow_id", 1, "int(11)", false, "PRI", null, "Flow key."));

    Schema schema = schema(
        """
            {
              "type":"record",
              "name":"KeyWidenRecord",
              "fields":[{"name":"FLOW_ID","type":"long"}]
            }
            """);

    IllegalStateException exception = assertThrows(
        IllegalStateException.class,
        () -> manager(jdbc, properties()).ensureSchema(schema));

    assertTrue(exception.getMessage().contains("key column flow_id"));
    assertTrue(jdbc.executedSql().isEmpty());
  }

  @Test
  void shouldRetryAddColumnAfterNonConnectionDataAccessFailure() {
    FakeJdbcTemplate jdbc = new FakeJdbcTemplate();
    jdbc.add(baseColumn("existing", "int(11)"));
    AtomicInteger attempts = new AtomicInteger();
    jdbc.onExecute(sql -> {
      if (attempts.getAndIncrement() == 0) {
        throw new DataAccessResourceFailureException("DDL temporarily rejected");
      }
      jdbc.add(baseColumn("new_field", "bigint(20)"));
    });

    Schema schema = schema(
        """
            {
              "type":"record",
              "name":"RetryAddRecord",
              "fields":[
                {"name":"EXISTING","type":"int"},
                {"name":"NEW_FIELD","type":"long"}
              ]
            }
            """);

    manager(jdbc, properties()).ensureSchema(schema);

    assertEquals(2, attempts.get());
    assertEquals(2, jdbc.executedSql().size());
  }

  @Test
  void shouldRetryModifyColumnAfterNonConnectionDataAccessFailure() {
    FakeJdbcTemplate jdbc = new FakeJdbcTemplate();
    jdbc.add(baseColumn("counter", "int(11)"));
    AtomicInteger attempts = new AtomicInteger();
    jdbc.onExecute(sql -> {
      if (attempts.getAndIncrement() == 0) {
        throw new DataAccessResourceFailureException("DDL temporarily rejected");
      }
      jdbc.replaceType("counter", "bigint(20)");
    });

    Schema schema = schema(
        """
            {
              "type":"record",
              "name":"RetryModifyRecord",
              "fields":[{"name":"COUNTER","type":"long"}]
            }
            """);

    manager(jdbc, properties()).ensureSchema(schema);

    assertEquals(2, attempts.get());
    assertEquals(2, jdbc.executedSql().size());
  }

  @Test
  void shouldPropagateJdbcConnectionFailureWithoutRetryingAddColumn() {
    FakeJdbcTemplate jdbc = new FakeJdbcTemplate();
    jdbc.add(baseColumn("existing", "int(11)"));
    CannotGetJdbcConnectionException connectionFailure =
        new CannotGetJdbcConnectionException("StarRocks unavailable");
    jdbc.onExecute(sql -> {
      throw connectionFailure;
    });

    Schema schema = schema(
        """
            {
              "type":"record",
              "name":"ConnectionFailureRecord",
              "fields":[
                {"name":"EXISTING","type":"int"},
                {"name":"NEW_FIELD","type":"long"}
              ]
            }
            """);

    CannotGetJdbcConnectionException actual = assertThrows(
        CannotGetJdbcConnectionException.class,
        () -> manager(jdbc, properties()).ensureSchema(schema));

    assertSame(connectionFailure, actual);
    assertEquals(1, jdbc.executedSql().size());
  }

  @Test
  void shouldPropagateJdbcConnectionFailureWithoutRetryingModifyColumn() {
    FakeJdbcTemplate jdbc = new FakeJdbcTemplate();
    jdbc.add(baseColumn("counter", "int(11)"));
    CannotGetJdbcConnectionException connectionFailure =
        new CannotGetJdbcConnectionException("StarRocks unavailable");
    jdbc.onExecute(sql -> {
      throw connectionFailure;
    });

    Schema schema = schema(
        """
            {
              "type":"record",
              "name":"ModifyConnectionFailureRecord",
              "fields":[{"name":"COUNTER","type":"long"}]
            }
            """);

    CannotGetJdbcConnectionException actual = assertThrows(
        CannotGetJdbcConnectionException.class,
        () -> manager(jdbc, properties()).ensureSchema(schema));

    assertSame(connectionFailure, actual);
    assertEquals(1, jdbc.executedSql().size());
  }

  @Test
  void shouldTimeOutWhenAddedColumnNeverBecomesVisible() {
    FakeJdbcTemplate jdbc = new FakeJdbcTemplate();
    jdbc.add(baseColumn("existing", "int(11)"));

    StarRocksProperties properties = properties();
    properties.setSchemaEvolutionTimeoutMs(1000);
    properties.setSchemaEvolutionPollMs(10);

    Schema schema = schema(
        """
            {
              "type":"record",
              "name":"AddTimeoutRecord",
              "fields":[
                {"name":"EXISTING","type":"int"},
                {"name":"NEW_FIELD","type":"long"}
              ]
            }
            """);

    IllegalStateException exception = assertThrows(
        IllegalStateException.class,
        () -> manager(jdbc, properties).ensureSchema(schema));

    assertTrue(exception.getMessage().contains("Timed out waiting for StarRocks schema evolution"));
    assertEquals(1, jdbc.executedSql().size());
  }

  @Test
  void shouldTimeOutWhenModifiedTypeNeverBecomesVisible() {
    FakeJdbcTemplate jdbc = new FakeJdbcTemplate();
    jdbc.add(baseColumn("counter", "int(11)"));

    StarRocksProperties properties = properties();
    properties.setSchemaEvolutionTimeoutMs(1000);
    properties.setSchemaEvolutionPollMs(10);

    Schema schema = schema(
        """
            {
              "type":"record",
              "name":"ModifyTimeoutRecord",
              "fields":[{"name":"COUNTER","type":"long"}]
            }
            """);

    IllegalStateException exception = assertThrows(
        IllegalStateException.class,
        () -> manager(jdbc, properties).ensureSchema(schema));

    assertTrue(exception.getMessage().contains("Timed out waiting for StarRocks type evolution"));
    assertEquals(1, jdbc.executedSql().size());
  }

  @Test
  void shouldFailWhenStarRocksTableDoesNotExist() {
    FakeJdbcTemplate jdbc = new FakeJdbcTemplate();

    IllegalStateException exception = assertThrows(
        IllegalStateException.class,
        () -> manager(jdbc, properties()).currentNetFlowColumns());

    assertEquals("StarRocks table does not exist: nimbo.netflow", exception.getMessage());
  }

  @Test
  void shouldRejectUnsafeDatabaseIdentifierBeforeExecutingDdl() {
    FakeJdbcTemplate jdbc = new FakeJdbcTemplate();
    jdbc.add(baseColumn("existing", "int(11)"));

    StarRocksProperties properties = properties();
    properties.setDatabase("nimbo-prod");

    Schema schema = schema(
        """
            {
              "type":"record",
              "name":"UnsafeIdentifierRecord",
              "fields":[
                {"name":"EXISTING","type":"int"},
                {"name":"NEW_FIELD","type":"long"}
              ]
            }
            """);

    IllegalStateException exception = assertThrows(
            IllegalStateException.class,
        () -> manager(jdbc, properties).ensureSchema(schema));

    assertEquals("Unsafe SQL identifier: nimbo-prod", exception.getMessage());
    assertTrue(jdbc.executedSql().isEmpty());
  }

  @Test
  void shouldRestoreInterruptFlagWhenInterruptedWhileWaitingForSchemaEvolution() {
    FakeJdbcTemplate jdbc = new FakeJdbcTemplate();
    jdbc.add(baseColumn("existing", "int(11)"));
    jdbc.onExecute(sql -> {
      // Keep the new column invisible so the manager enters its polling sleep.
    });

    StarRocksProperties properties = properties();
    properties.setSchemaEvolutionTimeoutMs(1000);
    properties.setSchemaEvolutionPollMs(100);

    Schema schema = schema(
        """
            {
              "type":"record",
              "name":"InterruptedRecord",
              "fields":[
                {"name":"EXISTING","type":"int"},
                {"name":"NEW_FIELD","type":"long"}
              ]
            }
            """);

    Thread.currentThread().interrupt();
    try {
      IllegalStateException exception = assertThrows(
          IllegalStateException.class,
          () -> manager(jdbc, properties).ensureSchema(schema));

      assertTrue(exception.getMessage().contains("Interrupted while waiting"));
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      assertTrue(Thread.interrupted());
      assertFalse(Thread.currentThread().isInterrupted());
    }
  }

  private static StarRocksSchemaEvolutionManager manager(
      FakeJdbcTemplate jdbc,
      StarRocksProperties properties) {
    return new StarRocksSchemaEvolutionManager(jdbc, properties, new SchemaMapping());
  }

  private static StarRocksProperties properties() {
    StarRocksProperties properties = new StarRocksProperties();
    properties.setDatabase("nimbo");
    properties.setTable("netflow");
    properties.setSchemaEvolutionTimeoutMs(250);
    properties.setSchemaEvolutionPollMs(1);
    return properties;
  }

  private static Schema schema(String json) {
    return new Schema.Parser().parse(json);
  }

  private static ColumnRow baseColumn(String name, String type) {
    return new ColumnRow(name, 1, type, true, "", null, "");
  }

  private static void addKafkaMetadataColumns(FakeJdbcTemplate jdbc) {
    jdbc.add(baseColumn("kafka_topic", "varchar(255)"));
    jdbc.add(baseColumn("kafka_partition", "int(11)"));
    jdbc.add(baseColumn("kafka_offset", "bigint(20)"));
    jdbc.add(baseColumn("PROCESS_TIME", "bigint(20)"));
  }

  private record ColumnRow(
      String name,
      int ordinalPosition,
      String columnType,
      boolean nullable,
      String columnKey,
      String defaultValue,
      String comment) {

    Object value(String columnLabel) {
      return switch (columnLabel) {
        case "COLUMN_NAME" -> name;
        case "ORDINAL_POSITION" -> ordinalPosition;
        case "COLUMN_TYPE" -> columnType;
        case "IS_NULLABLE" -> nullable ? "YES" : "NO";
        case "COLUMN_KEY" -> columnKey;
        case "COLUMN_DEFAULT" -> defaultValue;
        case "COLUMN_COMMENT" -> comment;
        default -> throw new IllegalArgumentException("Unexpected column label: " + columnLabel);
      };
    }
  }

  private record QueryCall(String sql, List<Object> args) {

    boolean isColumnLoadFor(String database, String table) {
      String normalized = sql.replaceAll("\\s+", " ").trim();
      return normalized.contains("FROM information_schema.columns")
          && normalized.contains("WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?")
          && normalized.endsWith("ORDER BY ORDINAL_POSITION")
          && args.equals(List.of(database, table));
    }
  }

  private static final class FakeJdbcTemplate extends JdbcTemplate {

    private final List<ColumnRow> columns = new ArrayList<>();
    private final List<String> executedSql = new ArrayList<>();
    private Consumer<String> executeHandler = sql -> {
    };
    private final List<QueryCall> queries = new ArrayList<>();
    private int queryCount;

    void add(ColumnRow column) {
      ColumnRow withOrdinal = column.ordinalPosition() > 1
          ? column
          : new ColumnRow(
              column.name(),
              nextOrdinal(),
              column.columnType(),
              column.nullable(),
              column.columnKey(),
              column.defaultValue(),
              column.comment());
      columns.add(withOrdinal);
    }

    int nextOrdinal() {
      return columns.size() + 1;
    }

    void replaceType(String name, String columnType) {
      for (int index = 0; index < columns.size(); index++) {
        ColumnRow current = columns.get(index);
        if (current.name().equalsIgnoreCase(name)) {
          columns.set(index, new ColumnRow(
              current.name(),
              current.ordinalPosition(),
              columnType,
              current.nullable(),
              current.columnKey(),
              current.defaultValue(),
              current.comment()));
          return;
        }
      }
      throw new IllegalArgumentException("Column not found: " + name);
    }

    void onExecute(Consumer<String> handler) {
      this.executeHandler = handler;
    }

    List<String> executedSql() {
      return List.copyOf(executedSql);
    }

    int queryCount() {
      return queryCount;
    }

    List<QueryCall> queries() {
      return List.copyOf(queries);
    }

    @Override
    public void execute(String sql) throws DataAccessException {
      executedSql.add(sql);
      executeHandler.accept(sql);
    }

    @Override
    public <T> List<T> query(String sql, RowMapper<T> rowMapper, Object... args)
        throws DataAccessException {
      queryCount++;
      QueryCall call = new QueryCall(sql, List.of(args));
      queries.add(call);
      if (!call.isColumnLoadFor("nimbo", "netflow")) {
        throw new IllegalArgumentException("Unexpected query: " + call);
      }
      List<T> mapped = new ArrayList<>();
      for (int index = 0; index < columns.size(); index++) {
        try {
          mapped.add(rowMapper.mapRow(resultSet(columns.get(index)), index));
        } catch (SQLException exception) {
          throw new DataAccessResourceFailureException("Failed to map fake result set", exception);
        }
      }
      return mapped;
    }

    private ResultSet resultSet(ColumnRow row) {
      return (ResultSet) Proxy.newProxyInstance(
          ResultSet.class.getClassLoader(),
          new Class<?>[]{ResultSet.class},
          (proxy, method, args) -> {
            if ("getString".equals(method.getName())) {
              Object value = row.value((String) args[0]);
              return value == null ? null : value.toString();
            }
            if ("getInt".equals(method.getName())) {
              Object value = row.value((String) args[0]);
              return value == null ? 0 : ((Number) value).intValue();
            }
            if ("toString".equals(method.getName())) {
              return "FakeResultSet[" + row.name() + "]";
            }
            throw new UnsupportedOperationException(
                "Unsupported ResultSet method in test: " + method.getName());
          });
    }
  }
}
