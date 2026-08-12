package com.nimbo.storage.service;

import com.nimbo.ingester.model.IngestedRecord;
import com.nimbo.storage.config.HdfsProperties;
import com.nimbo.storage.exception.StorageException;
import com.nimbo.storage.model.TopicPartitionKey;
import com.nimbo.storage.service.batching.BatchAccumulator;
import com.nimbo.storage.service.batching.RecordBatch;
import com.nimbo.storage.service.hdfs.FileStorage;
import com.nimbo.storage.service.naming.FileNameGenerator;
import com.nimbo.storage.service.parquet.ParquetFileWriter;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class NetFlowStorageTest {

    @Mock
    private BatchAccumulator batchAccumulator;

    @Mock
    private ParquetFileWriter parquetFileWriter;

    @Mock
    private FileStorage fileStorage;

    @Mock
    private FileNameGenerator fileNameGenerator;

    @Mock
    private HdfsProperties hdfsProperties;

    @Mock
    private IngestedRecord record;

    @Mock
    private RecordBatch batch;

    private NetFlowStorage storage;

    @BeforeEach
    void setUp() {
        when(hdfsProperties.getBasePath())
                .thenReturn("/netflow/parquet");

        storage = new NetFlowStorage(
                batchAccumulator,
                parquetFileWriter,
                fileStorage,
                fileNameGenerator,
                hdfsProperties
        );
    }

    @Test
    void acceptShouldAddRecordAndNotFlushWhenThresholdNotReached() {

        when(batchAccumulator.shouldFlush())
                .thenReturn(false);

        Optional<Map<TopicPartitionKey, Long>> result =
                storage.accept(record);

        assertTrue(result.isEmpty());

        verify(batchAccumulator).add(record);
        verify(batchAccumulator).shouldFlush();

        verifyNoInteractions(
                parquetFileWriter,
                fileStorage,
                fileNameGenerator
        );
    }

    @Test
    void acceptShouldFlushWhenThresholdIsReached() throws Exception {

        when(batchAccumulator.shouldFlush())
                .thenReturn(true);

        when(batchAccumulator.peekBatch())
                .thenReturn(batch);

        when(batch.getRecords())
                .thenReturn(List.of(record));

        when(record.partition())
                .thenReturn(2);

        when(fileNameGenerator.generateFileName(2))
                .thenReturn("netflow-2.parquet");

        when(fileNameGenerator.generateTempFileName("netflow-2.parquet"))
                .thenReturn("netflow-2.parquet.tmp");

        when(batch.getOffsetsToCommit())
                .thenReturn(Map.of(
                        new TopicPartitionKey("netflow", 2),
                        101L
                ));

        Map<TopicPartitionKey, Long> expectedOffsets =
                Map.of(
                        new TopicPartitionKey("netflow", 2),
                        101L
                );

        Optional<Map<TopicPartitionKey, Long>> result =
                storage.accept(record);

        assertTrue(result.isPresent());
        assertEquals(expectedOffsets, result.get());

        verify(batchAccumulator).add(record);
        verify(batchAccumulator).shouldFlush();
        verify(batchAccumulator).peekBatch();

        verify(fileStorage).ensureDirectoryExists(
                new Path("/netflow/parquet")
        );

        verify(parquetFileWriter).writeToPath(
                eq(batch),
                eq(new Path(
                        "/netflow/parquet",
                        "netflow-2.parquet.tmp"
                ))
        );

        verify(fileStorage).commitAtomic(
                new Path(
                        "/netflow/parquet",
                        "netflow-2.parquet.tmp"
                ),
                new Path(
                        "/netflow/parquet",
                        "netflow-2.parquet"
                )
        );

        verify(batch).getOffsetsToCommit();
        verify(batchAccumulator).clear();
    }

    @Test
    void flushShouldReturnEmptyMapWhenAccumulatorIsEmpty() {

        when(batchAccumulator.isEmpty())
                .thenReturn(true);

        Map<TopicPartitionKey, Long> result =
                storage.flush();

        assertTrue(result.isEmpty());

        verify(batchAccumulator).isEmpty();

        verifyNoInteractions(
                parquetFileWriter,
                fileStorage,
                fileNameGenerator
        );
    }

    @Test
    void flushShouldPersistBatchAndClearAccumulator() throws Exception {

        when(batchAccumulator.isEmpty())
                .thenReturn(false);

        when(batchAccumulator.getRecordCount())
                .thenReturn(5);

        when(batchAccumulator.peekBatch())
                .thenReturn(batch);

        when(batch.getRecords())
                .thenReturn(List.of(record));

        when(record.partition())
                .thenReturn(1);

        when(fileNameGenerator.generateFileName(1))
                .thenReturn("netflow-1.parquet");

        when(fileNameGenerator.generateTempFileName("netflow-1.parquet"))
                .thenReturn("netflow-1.parquet.tmp");

        Map<TopicPartitionKey, Long> expectedOffsets =
                Map.of(
                        new TopicPartitionKey("netflow", 1),
                        200L
                );

        when(batch.getOffsetsToCommit())
                .thenReturn(expectedOffsets);

        Map<TopicPartitionKey, Long> result =
                storage.flush();

        assertEquals(expectedOffsets, result);

        verify(fileStorage).ensureDirectoryExists(
                new Path("/netflow/parquet")
        );

        verify(parquetFileWriter).writeToPath(
                eq(batch),
                eq(new Path(
                        "/netflow/parquet",
                        "netflow-1.parquet.tmp"
                ))
        );

        verify(fileStorage).commitAtomic(
                new Path(
                        "/netflow/parquet",
                        "netflow-1.parquet.tmp"
                ),
                new Path(
                        "/netflow/parquet",
                        "netflow-1.parquet"
                )
        );

        verify(batch).getOffsetsToCommit();
        verify(batchAccumulator).clear();
    }

    @Test
    void shouldKeepBatchWhenParquetWriteFails() throws Exception {

        when(batchAccumulator.isEmpty())
                .thenReturn(false);

        when(batchAccumulator.getRecordCount())
                .thenReturn(5);

        when(batchAccumulator.peekBatch())
                .thenReturn(batch);

        when(batch.getRecords())
                .thenReturn(List.of(record));

        when(record.partition())
                .thenReturn(0);

        when(fileNameGenerator.generateFileName(0))
                .thenReturn("netflow-0.parquet");

        when(fileNameGenerator.generateTempFileName("netflow-0.parquet"))
                .thenReturn("netflow-0.parquet.tmp");

        doThrow(new RuntimeException("HDFS write failed"))
                .when(parquetFileWriter)
                .writeToPath(
                        eq(batch),
                        eq(new Path(
                                "/netflow/parquet",
                                "netflow-0.parquet.tmp"
                        ))
                );

        assertThrows(
                StorageException.class,
                () -> storage.flush()
        );

        verify(parquetFileWriter).writeToPath(
                eq(batch),
                eq(new Path(
                        "/netflow/parquet",
                        "netflow-0.parquet.tmp"
                ))
        );

        verify(fileStorage).cleanupTempFile(
                new Path(
                        "/netflow/parquet",
                        "netflow-0.parquet.tmp"
                )
        );

        verify(batchAccumulator, never()).clear();

        verify(fileStorage, never()).commitAtomic(
                any(Path.class),
                any(Path.class)
        );
    }

    @Test
    void shouldRejectNullRecord() {

        assertThrows(
                NullPointerException.class,
                () -> storage.accept(null)
        );

        verifyNoInteractions(batchAccumulator);
    }

    @Test
    void constructorShouldRejectNullDependencies() {

        assertThrows(
                NullPointerException.class,
                () -> new NetFlowStorage(
                        null,
                        parquetFileWriter,
                        fileStorage,
                        fileNameGenerator,
                        hdfsProperties
                )
        );

        assertThrows(
                NullPointerException.class,
                () -> new NetFlowStorage(
                        batchAccumulator,
                        null,
                        fileStorage,
                        fileNameGenerator,
                        hdfsProperties
                )
        );

        assertThrows(
                NullPointerException.class,
                () -> new NetFlowStorage(
                        batchAccumulator,
                        parquetFileWriter,
                        null,
                        fileNameGenerator,
                        hdfsProperties
                )
        );

        assertThrows(
                NullPointerException.class,
                () -> new NetFlowStorage(
                        batchAccumulator,
                        parquetFileWriter,
                        fileStorage,
                        null,
                        hdfsProperties
                )
        );

        assertThrows(
                NullPointerException.class,
                () -> new NetFlowStorage(
                        batchAccumulator,
                        parquetFileWriter,
                        fileStorage,
                        fileNameGenerator,
                        null
                )
        );
    }

}
