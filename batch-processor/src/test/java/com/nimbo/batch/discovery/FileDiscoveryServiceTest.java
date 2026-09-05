package com.nimbo.batch.discovery;

import com.nimbo.batch.config.BatchConfig;
import com.nimbo.batch.config.HdfsConfig;
import com.nimbo.batch.dao.BatchStateDao;
import com.nimbo.batch.model.BatchStateRecord;
import com.nimbo.batch.util.TestPostgresExtension;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
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
        batchConfig.setMaxFilesPerBatch(2);
        HdfsConfig hdfsConfig = new HdfsConfig();
        hdfsConfig.setUri("file://");
        hdfsConfig.setParquetPath(directory.toString());
        service = new FileDiscoveryService(fileSystem, hdfsConfig, batchConfig, stateDao);
    }

    @AfterEach
    void closeFileSystem() throws IOException {
        fileSystem.close();
    }

    @Test
    void givenNewAndInvalidFiles_whenDiscovered_thenOldestValidFilesRespectTheBatchLimit() throws IOException {
        // Given: three timestamped Parquet files plus entries that must be ignored
        createFile("netflow-p01-2026-08-29T10-30-00-000Z-third.parquet");
        createFile("netflow-p02-2026-08-29T08-30-00-000Z-first.parquet");
        createFile("netflow-p03-2026-08-29T09-30-00-000Z-second.parquet");
        createFile("notes.txt");
        createFile(".hidden.parquet");
        createFile("netflow-p01-invalid.parquet");

        // When: the service discovers a bounded batch
        List<Path> files = service.discoverNewFiles(UUID.fromString("10000000-0000-0000-0000-000000000001"));

        // Then: only the two oldest valid files are returned in timestamp order
        assertThat(files).extracting(Path::getName).containsExactly(
                "netflow-p02-2026-08-29T08-30-00-000Z-first.parquet",
                "netflow-p03-2026-08-29T09-30-00-000Z-second.parquet");
    }

    @Test
    void givenARecordedFile_whenDiscovered_thenItIsExcludedFromTheNextBatch() throws IOException {
        // Given: one previously recorded file and one new file
        String processed = "netflow-p01-2026-08-29T08-30-00-000Z-processed.parquet";
        String pending = "netflow-p02-2026-08-29T09-30-00-000Z-pending.parquet";
        createFile(processed);
        createFile(pending);
        stateDao.markFilesAsProcessed(List.of(BatchStateRecord.builder()
                .fileName(processed)
                .fileTimestamp(java.time.Instant.parse("2026-08-29T08:30:00Z"))
                .batchId(UUID.fromString("10000000-0000-0000-0000-000000000002"))
                .build()));

        // When: the service reads the real persisted processing state
        List<Path> files = service.discoverNewFiles(UUID.fromString("10000000-0000-0000-0000-000000000003"));

        // Then: only the unprocessed file is eligible
        assertThat(files).extracting(Path::getName).containsExactly(pending);
        assertThat(service.isUsingFallback()).isFalse();
    }

    @Test
    void givenValidFiles_whenMarkedProcessed_thenTheyArePersistedAndNotRediscovered() throws IOException {
        // Given: two valid files and a fixed batch identifier
        Path first = createFile("netflow-p01-2026-08-29T08-30-00-000Z-first.parquet");
        Path second = createFile("netflow-p02-2026-08-29T09-30-00-000Z-second.parquet");
        UUID batchId = UUID.fromString("10000000-0000-0000-0000-000000000004");

        // When: the files are marked through the real Jdbi DAO
        service.markFilesAsProcessed(List.of(first, second), batchId);

        // Then: both records are stored and discovery returns no files
        assertThat(stateDao.getProcessedFileNames()).containsExactlyInAnyOrder(first.getName(), second.getName());
        assertThat(service.discoverNewFiles(UUID.fromString("10000000-0000-0000-0000-000000000005"))).isEmpty();
    }

    @Test
    void givenMissingDiscoveryDirectory_whenDiscovered_thenTheFailureIsReported() {
        // Given: a service configured with a nonexistent directory
        HdfsConfig missingConfig = new HdfsConfig();
        missingConfig.setUri("file://");
        missingConfig.setParquetPath(directory.resolve("missing").toString());
        FileDiscoveryService missingService = new FileDiscoveryService(fileSystem, missingConfig, batchConfig, stateDao);

        // When: discovery attempts to list the missing location
        Throwable failure = org.assertj.core.api.Assertions.catchThrowable(
                () -> missingService.discoverNewFiles(UUID.fromString("10000000-0000-0000-0000-000000000006")));

        // Then: the infrastructure failure is exposed with its discovery context
        assertThat(failure).isInstanceOf(RuntimeException.class).hasMessageContaining("File discovery failed");
    }

    private Path createFile(String name) throws IOException {
        return new Path(Files.createFile(directory.resolve(name)).toUri());
    }
}
