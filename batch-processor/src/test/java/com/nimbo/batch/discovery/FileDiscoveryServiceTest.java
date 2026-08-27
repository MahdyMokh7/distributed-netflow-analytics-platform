package com.nimbo.batch.discovery;

import com.nimbo.batch.config.BatchConfig;
import com.nimbo.batch.config.HdfsConfig;
import com.nimbo.batch.dao.BatchStateDao;
import com.nimbo.batch.model.BatchStateRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FileDiscoveryServiceTest {

    @Mock
    private FileSystem fileSystem;

    @Mock
    private HdfsConfig hdfsConfig;

    @Mock
    private BatchConfig batchConfig;

    @Mock
    private BatchStateDao stateDao;

    private FileDiscoveryService service;

    @BeforeEach
    void setUp() {
        lenient().when(hdfsConfig.getFullParquetPath()).thenReturn("hdfs://test/path/");
        lenient().when(batchConfig.getMaxFilesPerBatch()).thenReturn(10);
        service = new FileDiscoveryService(fileSystem, hdfsConfig, batchConfig, stateDao);
    }

    @Test
    void shouldDiscoverNewFilesWhenNoneProcessed() throws IOException {
        Path basePath = new Path("hdfs://test/path/");
        Path file1 = new Path(basePath, "netflow-p01-2026-08-27T10-30-00-123Z-abc.parquet");
        Path file2 = new Path(basePath, "netflow-p02-2026-08-27T11-30-00-456Z-def.parquet");

        FileStatus status1 = mockFileStatus(file1);
        FileStatus status2 = mockFileStatus(file2);

        when(fileSystem.listStatus(basePath)).thenReturn(new FileStatus[]{status1, status2});
        when(stateDao.getProcessedFileNames()).thenReturn(new HashSet<>());

        UUID batchId = UUID.randomUUID();
        List<Path> result = service.discoverNewFiles(batchId);

        assertThat(result).hasSize(2);
        assertThat(result).containsExactly(file1, file2);
        verify(stateDao).getProcessedFileNames();
    }

    @Test
    void shouldFilterOutAlreadyProcessedFiles() throws IOException {
        Path basePath = new Path("hdfs://test/path/");
        Path processedFile = new Path(basePath, "netflow-p01-2026-08-27T10-30-00-123Z-abc.parquet");
        Path newFile = new Path(basePath, "netflow-p02-2026-08-27T11-30-00-456Z-def.parquet");

        FileStatus status1 = mockFileStatus(processedFile);
        FileStatus status2 = mockFileStatus(newFile);

        when(fileSystem.listStatus(basePath)).thenReturn(new FileStatus[]{status1, status2});

        Set<String> processed = new HashSet<>();
        processed.add("netflow-p01-2026-08-27T10-30-00-123Z-abc.parquet");
        when(stateDao.getProcessedFileNames()).thenReturn(processed);

        UUID batchId = UUID.randomUUID();
        List<Path> result = service.discoverNewFiles(batchId);

        assertThat(result).hasSize(1);
        assertThat(result).containsExactly(newFile);
    }

    @Test
    void shouldApplyMaxFilesPerBatchLimit() throws IOException {
        when(batchConfig.getMaxFilesPerBatch()).thenReturn(2);

        Path basePath = new Path("hdfs://test/path/");
        Path file1 = new Path(basePath, "netflow-p01-2026-08-27T10-00-00-123Z-abc.parquet");
        Path file2 = new Path(basePath, "netflow-p02-2026-08-27T10-15-00-456Z-def.parquet");
        Path file3 = new Path(basePath, "netflow-p03-2026-08-27T10-30-00-789Z-ghi.parquet");

        FileStatus status1 = mockFileStatus(file1);
        FileStatus status2 = mockFileStatus(file2);
        FileStatus status3 = mockFileStatus(file3);

        when(fileSystem.listStatus(basePath))
                .thenReturn(new FileStatus[]{status1, status2, status3});
        when(stateDao.getProcessedFileNames()).thenReturn(new HashSet<>());

        UUID batchId = UUID.randomUUID();
        List<Path> result = service.discoverNewFiles(batchId);

        assertThat(result).hasSize(2);
        assertThat(result).containsExactly(file1, file2);
    }

    @Test
    void shouldSortFilesByTimestampOldestFirst() throws IOException {
        Path basePath = new Path("hdfs://test/path/");
        Path file1 = new Path(basePath, "netflow-p01-2026-08-27T10-00-00-123Z-abc.parquet");
        Path file2 = new Path(basePath, "netflow-p02-2026-08-27T10-30-00-456Z-def.parquet");
        Path file3 = new Path(basePath, "netflow-p03-2026-08-27T10-15-00-789Z-ghi.parquet");

        FileStatus status1 = mockFileStatus(file1);
        FileStatus status2 = mockFileStatus(file2);
        FileStatus status3 = mockFileStatus(file3);

        when(fileSystem.listStatus(basePath))
                .thenReturn(new FileStatus[]{status1, status2, status3});
        when(stateDao.getProcessedFileNames()).thenReturn(new HashSet<>());

        UUID batchId = UUID.randomUUID();
        List<Path> result = service.discoverNewFiles(batchId);

        assertThat(result).containsExactly(file1, file3, file2);
    }

    @Test
    void shouldReturnEmptyListWhenAllFilesProcessed() throws IOException {
        Path basePath = new Path("hdfs://test/path/");
        Path file1 = new Path(basePath, "netflow-p01-2026-08-27T10-30-00-123Z-abc.parquet");
        Path file2 = new Path(basePath, "netflow-p02-2026-08-27T11-30-00-456Z-def.parquet");

        FileStatus status1 = mockFileStatus(file1);
        FileStatus status2 = mockFileStatus(file2);

        when(fileSystem.listStatus(basePath)).thenReturn(new FileStatus[]{status1, status2});

        Set<String> processed = new HashSet<>();
        processed.add("netflow-p01-2026-08-27T10-30-00-123Z-abc.parquet");
        processed.add("netflow-p02-2026-08-27T11-30-00-456Z-def.parquet");
        when(stateDao.getProcessedFileNames()).thenReturn(processed);

        UUID batchId = UUID.randomUUID();
        List<Path> result = service.discoverNewFiles(batchId);

        assertThat(result).isEmpty();
    }

    @Test
    void shouldUseInMemoryFallbackWhenPostgresDown() throws IOException {
        Path basePath = new Path("hdfs://test/path/");
        Path file1 = new Path(basePath, "netflow-p01-2026-08-27T10-30-00-123Z-abc.parquet");
        Path file2 = new Path(basePath, "netflow-p02-2026-08-27T11-30-00-456Z-def.parquet");

        FileStatus status1 = mockFileStatus(file1);
        FileStatus status2 = mockFileStatus(file2);

        when(fileSystem.listStatus(basePath)).thenReturn(new FileStatus[]{status1, status2});
        when(stateDao.getProcessedFileNames()).thenThrow(new RuntimeException("DB connection failed"));

        UUID batchId = UUID.randomUUID();
        List<Path> result = service.discoverNewFiles(batchId);

        assertThat(result).hasSize(2);
        assertThat(service.isUsingFallback()).isTrue();
    }

    @Test
    void shouldMarkFilesAsProcessedInMemoryFirst() {
        UUID batchId = UUID.randomUUID();

        Path file1 = new Path("hdfs://test/path/netflow-p01-2026-08-27T10-30-00-123Z-abc.parquet");
        Path file2 = new Path("hdfs://test/path/netflow-p02-2026-08-27T11-30-00-456Z-def.parquet");
        List<Path> files = List.of(file1, file2);

        service.markFilesAsProcessed(files, batchId);

        verify(stateDao).markFilesAsProcessed(anyList());
    }

    @Test
    void shouldNotMarkEmptyFileList() {
        UUID batchId = UUID.randomUUID();

        service.markFilesAsProcessed(List.of(), batchId);

        verify(stateDao, never()).markFilesAsProcessed(anyList());
    }

    @Test
    void shouldMarkFilesWithUnparseableTimestamp() {
        UUID batchId = UUID.randomUUID();

        Path file1 = new Path("hdfs://test/path/invalid-name.parquet");
        Path file2 = new Path("hdfs://test/path/netflow-p01-2026-08-27T10-30-00-123Z-abc.parquet");
        List<Path> files = List.of(file1, file2);

        service.markFilesAsProcessed(files, batchId);

        verify(stateDao).markFilesAsProcessed(anyList());
    }

    @Test
    void shouldKeepStateInMemoryWhenPostgresFailsOnMark() throws IOException {
        UUID batchId = UUID.randomUUID();

        Path file1 = new Path("hdfs://test/path/netflow-p01-2026-08-27T10-30-00-123Z-abc.parquet");
        Path file2 = new Path("hdfs://test/path/netflow-p02-2026-08-27T11-30-00-456Z-def.parquet");
        List<Path> files = List.of(file1, file2);

        doThrow(new RuntimeException("DB write failed"))
                .when(stateDao).markFilesAsProcessed(anyList());

        service.markFilesAsProcessed(files, batchId);

        assertThat(service.isUsingFallback()).isTrue();

        when(fileSystem.listStatus(any(Path.class))).thenReturn(new FileStatus[]{});
        when(stateDao.getProcessedFileNames()).thenThrow(new RuntimeException("DB read failed"));

        UUID newBatchId = UUID.randomUUID();
        List<Path> result = service.discoverNewFiles(newBatchId);

        assertThat(result).isEmpty();
        assertThat(service.isUsingFallback()).isTrue();
    }

    @Test
    void shouldThrowExceptionWhenHdfsListingFails() throws IOException {
        when(fileSystem.listStatus(any(Path.class))).thenThrow(new IOException("HDFS connection failed"));

        UUID batchId = UUID.randomUUID();
        assertThatThrownBy(() -> service.discoverNewFiles(batchId))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("File discovery failed");
    }

    @Test
    void shouldSyncMemoryWithPostgresOnRead() throws IOException {
        Path basePath = new Path("hdfs://test/path/");
        Path file1 = new Path(basePath, "netflow-p01-2026-08-27T10-30-00-123Z-abc.parquet");
        Path file2 = new Path(basePath, "netflow-p02-2026-08-27T11-30-00-456Z-def.parquet");

        FileStatus status1 = mockFileStatus(file1);
        FileStatus status2 = mockFileStatus(file2);

        when(fileSystem.listStatus(basePath)).thenReturn(new FileStatus[]{status1, status2});

        Set<String> dbFiles = new HashSet<>();
        dbFiles.add("netflow-p01-2026-08-27T10-30-00-123Z-abc.parquet");
        when(stateDao.getProcessedFileNames()).thenReturn(dbFiles);

        UUID batchId = UUID.randomUUID();
        List<Path> result = service.discoverNewFiles(batchId);

        assertThat(result).hasSize(1);
        assertThat(result).containsExactly(file2);
        assertThat(service.isUsingFallback()).isFalse();
    }

    @Test
    void shouldSkipTempAndNonParquetFiles() throws IOException {
        Path basePath = new Path("hdfs://test/path/");
        Path parquetFile = new Path(basePath, "netflow-p01-2026-08-27T10-30-00-123Z-abc.parquet");
        Path tempFile = new Path(basePath, ".temp.parquet.tmp");
        Path textFile = new Path(basePath, "readme.txt");

        FileStatus status1 = mockFileStatus(parquetFile);
        FileStatus status2 = mockFileStatus(tempFile);
        FileStatus status3 = mockFileStatus(textFile);

        when(fileSystem.listStatus(basePath))
                .thenReturn(new FileStatus[]{status1, status2, status3});
        when(stateDao.getProcessedFileNames()).thenReturn(new HashSet<>());

        UUID batchId = UUID.randomUUID();
        List<Path> result = service.discoverNewFiles(batchId);

        assertThat(result).hasSize(1);
        assertThat(result).containsExactly(parquetFile);
    }

    @Test
    void shouldExtractTimestampFromFilenamesAndFilterCorrectly() throws IOException {
        Path basePath = new Path("hdfs://test/path/");
        Path validFile = new Path(basePath, "netflow-p01-2026-08-27T10-30-00-123Z-abc.parquet");
        Path invalidFile = new Path(basePath, "invalid.parquet");

        FileStatus status1 = mockFileStatus(validFile);
        FileStatus status2 = mockFileStatus(invalidFile);

        when(fileSystem.listStatus(basePath)).thenReturn(new FileStatus[]{status1, status2});
        when(stateDao.getProcessedFileNames()).thenReturn(new HashSet<>());

        UUID batchId = UUID.randomUUID();
        List<Path> result = service.discoverNewFiles(batchId);

        assertThat(result).hasSize(1);
        assertThat(result).containsExactly(validFile);
    }

    private FileStatus mockFileStatus(Path path) {
        return new FileStatus() {
            @Override
            public Path getPath() {
                return path;
            }

            @Override
            public boolean isDirectory() {
                return false;
            }

            @Override
            public long getLen() {
                return 1024;
            }

            @Override
            public long getBlockSize() {
                return 128 * 1024 * 1024;
            }

            @Override
            public short getReplication() {
                return 1;
            }

            @Override
            public long getModificationTime() {
                return System.currentTimeMillis();
            }

            @Override
            public long getAccessTime() {
                return System.currentTimeMillis();
            }

            @Override
            public org.apache.hadoop.fs.permission.FsPermission getPermission() {
                return org.apache.hadoop.fs.permission.FsPermission.getDefault();
            }

            @Override
            public String getOwner() {
                return "test";
            }

            @Override
            public String getGroup() {
                return "test";
            }
        };
    }
}