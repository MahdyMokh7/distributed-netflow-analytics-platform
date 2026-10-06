package com.nimbo.backfill.support;

import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDate;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

public final class StarRocksContainerExtension implements BeforeAllCallback, BeforeEachCallback {

  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(StarRocksContainerExtension.class);
  private static final SharedStarRocks STARROCKS = new SharedStarRocks();

  @Override
  public void beforeAll(ExtensionContext context) {
    context
        .getRoot()
        .getStore(NAMESPACE)
        .getOrComputeIfAbsent(
            SharedStarRocks.class,
            ignored -> {
              STARROCKS.start();
              return STARROCKS;
            },
            SharedStarRocks.class);
  }

  @Override
  public void beforeEach(ExtensionContext context) {
    STARROCKS.resetSchema();
  }

  public Connection connection() throws SQLException {
    return STARROCKS.connection();
  }

  public String jdbcUrl() {
    return STARROCKS.jdbcUrl();
  }

  public String sourceBasePath(String sourceName) {
    return "file://" + STARROCKS.sourceRoot(sourceName);
  }

  public void replaceHourlyParquet(String sourceName, LocalDate day, int hour, Path parquetFile) {
    STARROCKS.replaceHourlyParquet(sourceName, day, hour, parquetFile);
  }

  private static final class SharedStarRocks implements ExtensionContext.Store.CloseableResource {

    private static final String IMAGE = "starrocks/allin1-ubuntu:4.0.12";
    private static final int FE_HTTP_PORT = 8030;
    private static final int BE_HTTP_PORT = 8040;
    private static final int MYSQL_PORT = 9030;
    private static final String DATABASE = "nimbo";
    private static final String TABLE = "netflow";
    private static final String SOURCE_DIRECTORY = "/tmp/nimbo-backfill-it";
    private static final Pattern SAFE_SOURCE_NAME = Pattern.compile("[a-z0-9-]+");

    private final GenericContainer<?> container =
        new GenericContainer<>(DockerImageName.parse(IMAGE))
            .withExposedPorts(FE_HTTP_PORT, BE_HTTP_PORT, MYSQL_PORT)
            .waitingFor(
                Wait.forHttp("/api/health")
                    .forPort(FE_HTTP_PORT)
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.ofMinutes(4)))
            .withReuse(true);

    private boolean started;

    synchronized void start() {
      if (started) {
        return;
      }
      container.start();
      started = true;
    }

    Connection connection() throws SQLException {
      return DriverManager.getConnection(jdbcUrl(), "root", "");
    }

    String jdbcUrl() {
      return adminJdbcUrl() + "/" + DATABASE;
    }

    String sourceRoot(String sourceName) {
      requireSafeSourceName(sourceName);
      return SOURCE_DIRECTORY + "/" + sourceName;
    }

    synchronized void replaceHourlyParquet(
        String sourceName, LocalDate day, int hour, Path parquetFile) {
      if (hour < 0 || hour > 23) {
        throw new IllegalArgumentException("hour must be between 0 and 23");
      }

      String sourceRoot = sourceRoot(sourceName);
      String hourlyDirectory =
          sourceRoot + "/" + day + "/" + String.format(Locale.ROOT, "%02d", hour);
      executeInContainer("rm", "-rf", sourceRoot);
      executeInContainer("mkdir", "-p", hourlyDirectory);
      container.copyFileToContainer(
          MountableFile.forHostPath(parquetFile), hourlyDirectory + "/records.parquet");
    }

    synchronized void resetSchema() {
      String createTableSql = loadCreateTableSql();
      AtomicReference<SQLException> lastFailure = new AtomicReference<>();
      try {
        await()
            .alias("StarRocks BE capacity and backfill integration-test schema")
            .pollInterval(Duration.ofSeconds(1))
            .atMost(Duration.ofMinutes(2))
            .until(
                () -> {
                  try (Connection connection =
                          DriverManager.getConnection(adminJdbcUrl(), "root", "");
                      Statement statement = connection.createStatement()) {
                    statement.execute("DROP DATABASE IF EXISTS " + DATABASE);
                    statement.execute("CREATE DATABASE " + DATABASE);
                    statement.execute(createTableSql);
                    return true;
                  } catch (SQLException exception) {
                    lastFailure.set(exception);
                    return false;
                  }
                });
      } catch (ConditionTimeoutException exception) {
        throw new IllegalStateException(
            "StarRocks backfill integration-test schema was not created",
            lastFailure.get() == null ? exception : lastFailure.get());
      }
    }

    @Override
    public synchronized void close() {
      // analytics-query is the final StarRocks test-container owner in the reactor.
      started = false;
    }

    private String adminJdbcUrl() {
      return "jdbc:mysql://" + container.getHost() + ":" + container.getMappedPort(MYSQL_PORT);
    }

    private String loadCreateTableSql() {
      try (InputStream input = getClass().getClassLoader().getResourceAsStream("sql/netflow.sql")) {
        if (input == null) {
          throw new IllegalStateException("Missing StarRocks schema resource: sql/netflow.sql");
        }
        String sql = new String(input.readAllBytes(), StandardCharsets.UTF_8);
        if (!sql.contains("BUCKETS 8") || !sql.contains("\"replication_num\" = \"2\"")) {
          throw new IllegalStateException(
              "StarRocks test schema overrides no longer match netflow.sql");
        }
        return sql.replace("BUCKETS 8", "BUCKETS 1")
            .replace("\"replication_num\" = \"2\"", "\"replication_num\" = \"1\"");
      } catch (IOException exception) {
        throw new IllegalStateException("Failed to load StarRocks schema", exception);
      }
    }

    private void executeInContainer(String... command) {
      try {
        Container.ExecResult result = container.execInContainer(command);
        if (result.getExitCode() != 0) {
          throw new IllegalStateException("Container command failed: " + result.getStderr());
        }
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(
            "Interrupted while preparing test Parquet files", exception);
      } catch (IOException exception) {
        throw new IllegalStateException("Could not prepare test Parquet files", exception);
      }
    }

    private void requireSafeSourceName(String sourceName) {
      if (sourceName == null || !SAFE_SOURCE_NAME.matcher(sourceName).matches()) {
        throw new IllegalArgumentException(
            "sourceName must contain only lowercase letters, digits, or '-'");
      }
    }
  }
}
