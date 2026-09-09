package com.nimbo.batch.discovery;

import com.nimbo.batch.config.BatchConfig;
import com.nimbo.batch.config.HdfsConfig;
import com.nimbo.batch.dao.BatchStateDao;
import com.nimbo.batch.model.BatchStateRecord;
import com.nimbo.batch.metrics.BatchProcessorMetrics;
import com.nimbo.batch.util.TestPostgresExtension;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jdbi.v3.core.Jdbi;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class FileDiscoveryServiceTest {

    @RegisterExtension
    static final TestPostgresExtension POSTGRES = new TestPostgresExtension();

    @TempDir
    java.nio.file.Path directory;

    private FileSystem fileSystem;
    private BatchStateDao stateDao;
    private BatchConfig batchConfig;
    private FileDiscoveryService service;
    private BatchProcessorMetrics metrics;
    private SimpleMeterRegistry meterRegistry;

    @BeforeEach
    void setUp() throws IOException {
        fileSystem = FileSystem.getLocal(new Configuration());
        Jdbi jdbi = POSTGRES.jdbi();
        jdbi.useHandle(handle -> {
            handle.execute("CREATE TABLE IF NOT EXISTS batch_processor_state (file_name VARCHAR PRIMARY KEY, file_timestamp TIMESTAMP NOT NULL, batch_id UUID NOT NULL, processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)");
            handle.execute("TRUNCATE batch_processor_state");
        });
        stateDao = jdbi.onDemand(BatchStateDao.class);
        batchConfig = new BatchConfig();
        batchConfig.setMaxFilesPerBatch(3);
        HdfsConfig hdfsConfig = new HdfsConfig();
        hdfsConfig.setUri("file://");
        hdfsConfig.setParquetPath(directory.toString());
        meterRegistry = new SimpleMeterRegistry();
        metrics = new BatchProcessorMetrics(meterRegistry);
        service = new FileDiscoveryService(fileSystem, hdfsConfig, batchConfig, stateDao, metrics);
    }

    @AfterEach
    void closeFileSystem() throws IOException {
        fileSystem.close();
    }

    @Test
    void givenFilesAcrossUnorderedHourDirectories_whenDiscovered_thenTheGloballyOldestFilesFillTheBatch() throws IOException {
        // Given: four partitioned files created in an order unrelated to their timestamps
        createFile("2026-09-08", "00", "netflow-p01-2026-09-08T00-05-00-000Z-fourth.parquet");
        createFile("2026-09-07", "14", "netflow-p01-2026-09-07T14-40-00-000Z-third.parquet");
        createFile("2026-09-07", "13", "netflow-p02-2026-09-07T13-45-00-000Z-second.parquet");
        createFile("2026-09-07", "13", "netflow-p03-2026-09-07T13-10-00-000Z-first.parquet");

        // When: discovery fills a three-file batch
        List<Path> files = service.discoverNewFiles(UUID.fromString("10000000-0000-0000-0000-000000000001"));

        // Then: directory time is primary and filename time orders files within an hour
        assertThat(files).extracting(Path::getName).containsExactly(
                "netflow-p03-2026-09-07T13-10-00-000Z-first.parquet",
                "netflow-p02-2026-09-07T13-45-00-000Z-second.parquet",
                "netflow-p01-2026-09-07T14-40-00-000Z-third.parquet"
        );
    }

    @Test
    void givenProcessedAndMalformedEntries_whenDiscovered_thenOnlyValidPendingPartitionedFilesAreReturned() throws IOException {
        // Given: processed, temporary, misplaced, malformed, nested, and root-level files
        String processed = "netflow-p01-2026-09-07T13-10-00-000Z-processed.parquet";
        String pending = "netflow-p02-2026-09-07T13-20-00-000Z-pending.parquet";
        createFile("2026-09-07", "13", processed);
        createFile("2026-09-07", "13", pending);
        createFile("2026-09-07", "13", ".netflow-p03-2026-09-07T13-30-00-000Z-open.parquet.tmp");
        createFile("2026-09-07", "13", "netflow-p04-2026-09-07T14-00-00-000Z-misplaced.parquet");
        createFile("invalid-date", "13", "netflow-p05-2026-09-07T13-40-00-000Z-invalid-date.parquet");
        createFile("2026-09-07", "24", "netflow-p06-2026-09-07T23-40-00-000Z-invalid-hour.parquet");
        Files.createDirectories(directory.resolve("2026-09-07/13/nested.parquet"));
        Files.createFile(directory.resolve("netflow-p07-2026-09-07T12-00-00-000Z-root.parquet"));
        stateDao.markFilesAsProcessed(List.of(BatchStateRecord.builder()
                .fileName(processed)
                .fileTimestamp(Instant.parse("2026-09-07T13:10:00Z"))
                .batchId(UUID.fromString("10000000-0000-0000-0000-000000000002"))
                .build()));

        // When: the hierarchical layout is discovered
        List<Path> files = service.discoverNewFiles(UUID.fromString("10000000-0000-0000-0000-000000000003"));

        // Then: only the valid unprocessed file from the new layout is returned
        assertThat(files).extracting(Path::getName).containsExactly(pending);
        assertThat(service.isUsingFallback()).isFalse();
    }

    @Test
    void givenAValidPartitionedBatch_whenMarkedProcessed_thenItIsDurableAndNotRediscovered() throws IOException {
        // Given: two completed files in consecutive hourly directories
        Path first = createFile("2026-09-07", "13", "netflow-p01-2026-09-07T13-10-00-000Z-first.parquet");
        Path second = createFile("2026-09-07", "14", "netflow-p02-2026-09-07T14-10-00-000Z-second.parquet");
        UUID batchId = UUID.fromString("10000000-0000-0000-0000-000000000004");

        // When: the files are marked through the real Jdbi DAO
        service.markFilesAsProcessed(List.of(first, second), batchId);

        // Then: both names are stored and neither file is discovered again
        assertThat(stateDao.getProcessedFileNames()).containsExactlyInAnyOrder(first.getName(), second.getName());
        assertThat(service.discoverNewFiles(UUID.fromString("10000000-0000-0000-0000-000000000005"))).isEmpty();
        assertThat(meterRegistry.get("batch.processor.files.processed").counter().count()).isEqualTo(2.0);
        assertThat(meterRegistry.get("batch.processor.last.processed.file.timestamp.seconds").gauge().value())
                .isEqualTo(Instant.parse("2026-09-07T14:10:00Z").getEpochSecond());
    }

    @Test
    void givenMissingDiscoveryRoot_whenDiscovered_thenTheFailureIncludesDiscoveryContext() {
        // Given: a service configured with a nonexistent partition root
        HdfsConfig missingConfig = new HdfsConfig();
        missingConfig.setUri("file://");
        missingConfig.setParquetPath(directory.resolve("missing").toString());
        FileDiscoveryService missingService = new FileDiscoveryService(fileSystem, missingConfig, batchConfig, stateDao, metrics);

        // When: hierarchical discovery attempts to list the missing root
        Throwable failure = org.assertj.core.api.Assertions.catchThrowable(
                () -> missingService.discoverNewFiles(UUID.fromString("10000000-0000-0000-0000-000000000006")));

        // Then: the infrastructure failure is exposed with its discovery context
        assertThat(failure).isInstanceOf(RuntimeException.class).hasMessageContaining("File discovery failed");
    }

    private Path createFile(String date, String hour, String name) throws IOException {
        java.nio.file.Path hourDirectory = Files.createDirectories(directory.resolve(date).resolve(hour));
        return new Path(Files.createFile(hourDirectory.resolve(name)).toUri());
    }
}
