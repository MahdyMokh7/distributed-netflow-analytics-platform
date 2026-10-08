package com.nimbo.cubemaker.retention;

import com.nimbo.cubemaker.config.BatchConfig;
import com.nimbo.cubemaker.discovery.NetflowFileTimestampParser;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.jdbi.v3.core.Jdbi;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class RetentionMaintenanceService {

  private static final List<String> AGGREGATE_TABLES =
      List.of(
          "traffic_aggregates",
          "connection_aggregates",
          "protocol_usage_aggregates",
          "ip_bandwidth_aggregates");
  private static final DateTimeFormatter PARTITION_SUFFIX =
      DateTimeFormatter.ofPattern("uuuu_MM_dd");

  private final BatchConfig batchConfig;
  private final RetentionPolicy retentionPolicy;
  private final Jdbi jdbi;

  public RetentionMaintenanceService(
      BatchConfig batchConfig, RetentionPolicy retentionPolicy, Jdbi jdbi) {
    this.batchConfig = batchConfig;
    this.retentionPolicy = retentionPolicy;
    this.jdbi = jdbi;
  }

  public RetentionWindow maintain() {
    RetentionWindow window = retentionPolicy.currentWindow();
    if (!batchConfig.getRetention().isEnabled()) {
      return window;
    }

    maintainDatabasePartitions(window);
    return window;
  }

  public void ensurePartitionsForFiles(RetentionWindow window, List<Path> files) {
    if (files.isEmpty()) {
      return;
    }

    List<LocalDate> requiredDates =
        files.stream()
            .map(Path::getName)
            .map(NetflowFileTimestampParser::parse)
            .flatMap(java.util.Optional::stream)
            .filter(window::retains)
            .map(timestamp -> timestamp.atZone(java.time.ZoneOffset.UTC).toLocalDate())
            .distinct()
            .toList();

    if (requiredDates.isEmpty()) {
      return;
    }

    jdbi.useTransaction(
        handle -> {
          handle.execute("SELECT pg_advisory_xact_lock(hashtext(?))", "nimbo-aggregate-retention");
          for (String table : AGGREGATE_TABLES) {
            for (LocalDate date : requiredDates) {
              createDailyPartition(handle, table, date);
            }
          }
        });
  }

  private void maintainDatabasePartitions(RetentionWindow window) {
    LocalDate today = window.cutoffDate().plusDays(batchConfig.getRetention().getPreviousDays());
    jdbi.useTransaction(
        handle -> {
          handle.execute("SELECT pg_advisory_xact_lock(hashtext(?))", "nimbo-aggregate-retention");
          for (String table : AGGREGATE_TABLES) {
            List<String> partitions =
                handle
                    .createQuery(
                        """
                        SELECT child.relname
                        FROM pg_inherits inheritance
                        JOIN pg_class parent ON parent.oid = inheritance.inhparent
                        JOIN pg_class child ON child.oid = inheritance.inhrelid
                        WHERE parent.relname = :parent
                        """)
                    .bind("parent", table)
                    .mapTo(String.class)
                    .list();

            for (String partition : partitions) {
              LocalDate partitionDate = partitionDate(table, partition);
              if (partitionDate != null && partitionDate.isBefore(window.cutoffDate())) {
                handle.execute("DROP TABLE " + quoteIdentifier(partition));
              }
            }

            LocalDate date = window.cutoffDate();
            while (!date.isAfter(today)) {
              createDailyPartition(handle, table, date);
              date = date.plusDays(1);
            }
          }
        });
  }

  private void createDailyPartition(org.jdbi.v3.core.Handle handle, String table, LocalDate date) {
    String partition = table + "_" + PARTITION_SUFFIX.format(date);
    String sql =
        "CREATE TABLE IF NOT EXISTS "
            + quoteIdentifier(partition)
            + " PARTITION OF "
            + quoteIdentifier(table)
            + " FOR VALUES FROM ('"
            + date
            + "') TO ('"
            + date.plusDays(1)
            + "')";
    handle.execute(sql);
  }

  private LocalDate partitionDate(String parent, String partition) {
    String prefix = parent + "_";
    if (!partition.startsWith(prefix)) {
      return null;
    }
    try {
      return LocalDate.parse(partition.substring(prefix.length()), PARTITION_SUFFIX);
    } catch (DateTimeParseException exception) {
      log.warn("Ignoring aggregate partition with an unexpected name: {}", partition);
      return null;
    }
  }

  private String quoteIdentifier(String identifier) {
    return '"' + identifier.replace("\"", "\"\"") + '"';
  }
}
