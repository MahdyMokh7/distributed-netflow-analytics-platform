package com.nimbo.storage.service;

import com.nimbo.ingester.model.IngestedRecord;
import com.nimbo.storage.config.HdfsProperties;
import com.nimbo.storage.exception.StorageException;
import com.nimbo.storage.model.TopicPartitionKey;
import com.nimbo.storage.service.batching.BatchAccumulator;
import com.nimbo.storage.service.hdfs.FileStorage;
import com.nimbo.storage.service.naming.FileNameGenerator;
import com.nimbo.storage.service.parquet.ParquetFileWriter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

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
    private GenericRecord genericRecord;

    @Mock
    private ParquetWriter<GenericRecord> writer;

    private Schema schema;
    private NetFlowStorage storage;

    @BeforeEach
    void setUp() {
        when(hdfsProperties.getBasePath())
                .thenReturn("/netflow/parquet");

        schema = new Schema.Parser().parse(
                """
                {
                  "type": "record",
                  "name": "TestRecord",
                  "fields": [
                    {"name": "value", "type": "string"}
                  ]
                }
                """
        );

        storage = new NetFlowStorage(
                batchAccumulator,
                parquetFileWriter,
                fileStorage,
                fileNameGenerator,
                hdfsProperties
        );
    }

    private void mockOpenWriter(int partition, String fileName, String tempFileName) throws Exception {
        when(record.partition()).thenReturn(partition);
        when(record.record()).thenReturn(genericRecord);
        when(genericRecord.getSchema()).thenReturn(schema);

        when(fileNameGenerator.generateFileName(partition))
                .thenReturn(fileName);
        when(fileNameGenerator.generateTempFileName(fileName))
                .thenReturn(tempFileName);

        when(parquetFileWriter.createWriter(
                eq(new Path("/netflow/parquet", tempFileName)),
                eq(schema)
        )).thenReturn(writer);
    }

    @Test
    void acceptShouldAddRecordAndNotFlushWhenThresholdNotReached() throws Exception {
        mockOpenWriter(0, "netflow-0.parquet", "netflow-0.parquet.tmp");

        when(batchAccumulator.shouldFlush()).thenReturn(false);

        Optional<Map<TopicPartitionKey, Long>> result =
                storage.accept(record);

        assertTrue(result.isEmpty());

        verify(fileStorage).ensureDirectoryExists(new Path("/netflow/parquet"));
        verify(parquetFileWriter).createWriter(
                new Path("/netflow/parquet", "netflow-0.parquet.tmp"),
                schema
        );
        verify(parquetFileWriter).write(writer, genericRecord);

        verify(batchAccumulator).add(record);
        verify(batchAccumulator).shouldFlush();

        verify(parquetFileWriter, never()).closeWriter(any());
        verify(fileStorage, never()).commitAtomic(any(), any());
        verify(batchAccumulator, never()).clear();
    }

    @Test
    void acceptShouldFlushWhenThresholdIsReached() throws Exception {
        mockOpenWriter(2, "netflow-2.parquet", "netflow-2.parquet.tmp");

        when(batchAccumulator.shouldFlush()).thenReturn(true);

        Map<TopicPartitionKey, Long> expectedOffsets = Map.of(
                new TopicPartitionKey("netflow", 2), 101L
        );
        when(batchAccumulator.getPendingOffsets()).thenReturn(expectedOffsets);

        Optional<Map<TopicPartitionKey, Long>> result =
                storage.accept(record);

        assertTrue(result.isPresent());
        assertEquals(expectedOffsets, result.get());

        verify(fileStorage).ensureDirectoryExists(new Path("/netflow/parquet"));
        verify(parquetFileWriter).createWriter(
                new Path("/netflow/parquet", "netflow-2.parquet.tmp"),
                schema
        );
        verify(parquetFileWriter).write(writer, genericRecord);
        verify(batchAccumulator).add(record);
        verify(batchAccumulator).shouldFlush();

        verify(parquetFileWriter).closeWriter(writer);
        verify(fileStorage).commitAtomic(
                new Path("/netflow/parquet", "netflow-2.parquet.tmp"),
                new Path("/netflow/parquet", "netflow-2.parquet")
        );
        verify(batchAccumulator).getPendingOffsets();
        verify(batchAccumulator).clear();
    }

    @Test
    void flushShouldReturnEmptyMapWhenAccumulatorIsEmpty() {
        when(batchAccumulator.isEmpty()).thenReturn(true);

        Map<TopicPartitionKey, Long> result = storage.flush();

        assertTrue(result.isEmpty());
        verify(batchAccumulator).isEmpty();
        verifyNoInteractions(parquetFileWriter, fileStorage, fileNameGenerator);
    }

    @Test
    void flushShouldPersistCurrentWriterAndClearAccumulator() throws Exception {
        mockOpenWriter(1, "netflow-1.parquet", "netflow-1.parquet.tmp");
        when(batchAccumulator.shouldFlush()).thenReturn(false);
        storage.accept(record);
        reset(parquetFileWriter, fileStorage, batchAccumulator);

        when(batchAccumulator.isEmpty()).thenReturn(false);
        when(batchAccumulator.getRecordCount()).thenReturn(5L);

        Map<TopicPartitionKey, Long> expectedOffsets = Map.of(
                new TopicPartitionKey("netflow", 1), 200L
        );
        when(batchAccumulator.getPendingOffsets()).thenReturn(expectedOffsets);

        Map<TopicPartitionKey, Long> result = storage.flush();

        assertEquals(expectedOffsets, result);

        verify(parquetFileWriter).closeWriter(writer);
        verify(fileStorage).commitAtomic(
                new Path("/netflow/parquet", "netflow-1.parquet.tmp"),
                new Path("/netflow/parquet", "netflow-1.parquet")
        );
        verify(batchAccumulator).getPendingOffsets();
        verify(batchAccumulator).clear();
    }

    @Test
    void shouldCleanupTempFileAndClearAccumulatorWhenWriteFails() throws Exception {
        mockOpenWriter(0, "netflow-0.parquet", "netflow-0.parquet.tmp");

        doThrow(new RuntimeException("write failed"))
                .when(parquetFileWriter)
                .write(eq(writer), eq(genericRecord));

        assertThrows(StorageException.class, () -> storage.accept(record));

        verify(fileStorage).cleanupTempFile(
                new Path("/netflow/parquet", "netflow-0.parquet.tmp")
        );
        verify(batchAccumulator).clear();
        verify(fileStorage, never()).commitAtomic(any(), any());
    }

    @Test
    void shouldRejectNullRecord() {
        assertThrows(NullPointerException.class, () -> storage.accept(null));
        verifyNoInteractions(batchAccumulator, parquetFileWriter, fileStorage, fileNameGenerator);
    }

    @Test
    void constructorShouldRejectNullDependencies() {
        assertThrows(NullPointerException.class,
                () -> new NetFlowStorage(null, parquetFileWriter, fileStorage, fileNameGenerator, hdfsProperties));
        assertThrows(NullPointerException.class,
                () -> new NetFlowStorage(batchAccumulator, null, fileStorage, fileNameGenerator, hdfsProperties));
        assertThrows(NullPointerException.class,
                () -> new NetFlowStorage(batchAccumulator, parquetFileWriter, null, fileNameGenerator, hdfsProperties));
        assertThrows(NullPointerException.class,
                () -> new NetFlowStorage(batchAccumulator, parquetFileWriter, fileStorage, null, hdfsProperties));
        assertThrows(NullPointerException.class,
                () -> new NetFlowStorage(batchAccumulator, parquetFileWriter, fileStorage, fileNameGenerator, null));
    }
}