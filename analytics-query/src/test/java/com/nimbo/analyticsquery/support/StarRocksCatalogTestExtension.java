package com.nimbo.analyticsquery.support;

import static org.awaitility.Awaitility.await;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public final class StarRocksCatalogTestExtension
    implements BeforeAllCallback, BeforeEachCallback, ParameterResolver {

  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(StarRocksCatalogTestExtension.class);

  private static final Infrastructure INFRASTRUCTURE = new Infrastructure();

  @Override
  public void beforeAll(ExtensionContext context) {
    context
        .getRoot()
        .getStore(NAMESPACE)
        .getOrComputeIfAbsent(
            Infrastructure.class,
            ignored -> {
              INFRASTRUCTURE.start();
              return INFRASTRUCTURE;
            },
            Infrastructure.class);
  }

  @Override
  public void beforeEach(ExtensionContext context) {
    INFRASTRUCTURE.resetSchema();
  }

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext) {

    return parameterContext.getParameter().getType() == StarRocksCatalogTestExtension.class;
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext) {

    return this;
  }

  public String jdbcUrl() {
    return INFRASTRUCTURE.jdbcUrl();
  }

  public void dropTable() throws SQLException {
    INFRASTRUCTURE.execute("DROP TABLE nimbo.netflow");
  }

  private static final class Infrastructure implements ExtensionContext.Store.CloseableResource {

    private static final int FE_HTTP_PORT = 8030;
    private static final int MYSQL_PORT = 9030;

    private final GenericContainer<?> container =
        new GenericContainer<>(DockerImageName.parse("starrocks/allin1-ubuntu:4.0.12"))
            .withExposedPorts(FE_HTTP_PORT, MYSQL_PORT)
            .waitingFor(
                Wait.forHttp("/api/health")
                    .forPort(FE_HTTP_PORT)
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.ofMinutes(4)));

    private boolean started;

    synchronized void start() {
      if (!started) {
        container.start();
        started = true;
      }
    }

    void resetSchema() {
      await()
          .alias("StarRocks catalog test schema")
          .pollInterval(Duration.ofSeconds(1))
          .atMost(Duration.ofMinutes(2))
          .until(
              () -> {
                try {
                  execute("DROP DATABASE IF EXISTS nimbo");
                  execute("CREATE DATABASE nimbo");
                  execute(
                      """
                      CREATE TABLE nimbo.netflow (
                          PROCESS_TIME BIGINT NOT NULL COMMENT 'Kafka event timestamp',
                          ipv4_src_addr VARCHAR(15) NULL COMMENT 'Source IPv4 address',
                          ipv4_dst_addr VARCHAR(15) NULL COMMENT 'Destination IPv4 address',
                          in_bytes BIGINT NULL COMMENT 'Inbound bytes',
                          min_ttl INT NULL COMMENT 'Minimum observed flow TTL',
                          dns_query_type INT NULL COMMENT 'DNS query type',
                          new_flow_bytes BIGINT NULL COMMENT 'New flow byte count'
                      )
                      ENGINE = OLAP
                      DUPLICATE KEY(PROCESS_TIME)
                      DISTRIBUTED BY HASH(PROCESS_TIME)
                      BUCKETS 1
                      PROPERTIES ("replication_num" = "1")
                      """);
                  return true;
                } catch (SQLException exception) {
                  return false;
                }
              });

      await()
          .alias("StarRocks catalog test EXPLAIN readiness")
          .pollInterval(Duration.ofSeconds(1))
          .atMost(Duration.ofMinutes(2))
          .until(
              () -> {
                try {
                  execute(
                      """
                      EXPLAIN SELECT ipv4_src_addr, in_bytes
                      FROM nimbo.netflow
                      WHERE process_time >= 1791014400000
                        AND process_time < 1791018000000
                      LIMIT 10
                      """);
                  return true;
                } catch (SQLException exception) {
                  return false;
                }
              });
    }

    String jdbcUrl() {
      return "jdbc:mysql://" + container.getHost() + ":" + container.getMappedPort(MYSQL_PORT);
    }

    void execute(String sql) throws SQLException {
      try (Connection connection = DriverManager.getConnection(jdbcUrl(), "root", "");
          Statement statement = connection.createStatement()) {

        statement.execute(sql);
      }
    }

    @Override
    public synchronized void close() {
      if (container.isRunning()) {
        container.stop();
      }
      started = false;
    }
  }
}
