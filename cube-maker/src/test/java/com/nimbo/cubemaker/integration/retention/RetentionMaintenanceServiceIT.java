package com.nimbo.cubemaker.retention;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbo.cubemaker.config.BatchConfig;
import com.nimbo.cubemaker.config.HdfsConfig;
import com.nimbo.cubemaker.support.PostgresContainerExtension;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

class RetentionMaintenanceServiceIT {

  @RegisterExtension
  static final PostgresContainerExtension POSTGRES = new PostgresContainerExtension();

  @TempDir java.nio.file.Path directory;

  private FileSystem fileSystem;
  private Jdbi jdbi;
  private RetentionMaintenanceService service;

  @BeforeEach
  void setUp() throws IOException {
    fileSystem = FileSystem.getLocal(new Configuration());
    jdbi = POSTGRES.jdbi();

    BatchConfig batchConfig = new BatchConfig();
    batchConfig.getRetention().setEnabled(true);
    batchConfig.getRetention().setPreviousDays(3);
    RetentionPolicy policy =
        new RetentionPolicy(
            batchConfig, Clock.fixed(Instant.parse("2026-09-25T15:45:00Z"), ZoneOffset.UTC));

    HdfsConfig hdfsConfig = new HdfsConfig();
    hdfsConfig.setUri("file://");
    hdfsConfig.setParquetPath(directory.resolve("parquet").toString());
    hdfsConfig.setSparkHistoryPath(directory.resolve("spark-history").toString());
    service = new RetentionMaintenanceService(batchConfig, policy, jdbi, fileSystem, hdfsConfig);
  }

  @AfterEach
  void closeFileSystem() throws IOException {
    fileSystem.close();
  }

  @Test
  void
      givenExpiredAndRetainedDatabaseAndHdfsEntries_whenMaintained_thenOnlyExpiredDatabaseAndHistoryEntriesAreRemoved()
          throws IOException {
    // Given: daily partitions, retained and expired Parquet files, completed histories, and an
    // active history
    java.nio.file.Path expiredParquet =
        createParquet("2026-09-21", "23", "netflow-p01-2026-09-21T23-59-59-999Z-expired.parquet");
    java.nio.file.Path retainedParquet =
        createParquet("2026-09-22", "00", "netflow-p02-2026-09-22T00-00-00-000Z-retained.parquet");
    java.nio.file.Path futureParquet =
        createParquet("2026-10-02", "12", "netflow-p03-2026-10-02T12-00-00-000Z-future.parquet");
    java.nio.file.Path expiredHistory = createHistory("completed-old", "2026-09-21T23:00:00Z");
    java.nio.file.Path retainedHistory = createHistory("completed-current", "2026-09-22T00:00:00Z");
    java.nio.file.Path activeHistory = createHistory("active.inprogress", "2026-09-20T00:00:00Z");

    // When: one retention run uses the captured UTC calendar-day boundary
    RetentionWindow window = service.maintain();
    service.ensurePartitionsForFiles(window, List.of(new Path(futureParquet.toUri())));

    // Then: expired database partitions and completed history are removed; Parquet is retained for
    // discovery filtering
    assertThat(window.cutoffInstant()).isEqualTo(Instant.parse("2026-09-22T00:00:00Z"));
    assertThat(expiredParquet).exists();
    assertThat(retainedParquet).exists();
    assertThat(expiredHistory).doesNotExist();
    assertThat(retainedHistory).exists();
    assertThat(activeHistory).exists();
    assertThat(regclass("traffic_aggregates_2026_09_21")).isNull();
    assertThat(regclass("traffic_aggregates_2026_09_22")).isNotNull();
    assertThat(regclass("traffic_aggregates_2026_09_25")).isNotNull();
    assertThat(regclass("traffic_aggregates_2026_10_02")).isNotNull();
  }

  private java.nio.file.Path createParquet(String date, String hour, String name)
      throws IOException {
    java.nio.file.Path hourDirectory =
        Files.createDirectories(directory.resolve("parquet").resolve(date).resolve(hour));
    return Files.createFile(hourDirectory.resolve(name));
  }

  private java.nio.file.Path createHistory(String name, String modificationTime)
      throws IOException {
    java.nio.file.Path historyDirectory =
        Files.createDirectories(directory.resolve("spark-history"));
    java.nio.file.Path history = Files.createFile(historyDirectory.resolve(name));
    Files.setLastModifiedTime(history, FileTime.from(Instant.parse(modificationTime)));
    return history;
  }

  private String regclass(String relation) {
    return jdbi.withHandle(
        handle ->
            handle
                .createQuery("SELECT to_regclass(:relation)::text")
                .bind("relation", relation)
                .mapTo(String.class)
                .findOne()
                .orElse(null));
  }
}
