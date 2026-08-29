package com.nimbo.storage.service;

import com.nimbo.ingester.model.IngestedRecord;
import com.nimbo.storage.config.HdfsProperties;
import com.nimbo.storage.exception.ParquetWriteException;
import com.nimbo.storage.exception.StorageException;
import com.nimbo.storage.model.TopicPartitionKey;
import com.nimbo.storage.service.batching.BatchPolicy;
import com.nimbo.storage.service.hdfs.FileStorage;
import com.nimbo.storage.service.naming.FileNameGenerator;
import com.nimbo.storage.service.parquet.ParquetFileWriter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class NetFlowStorageTest {

    @Mock
    private BatchPolicy batchPolicy;

    @Mock
    private ParquetFileWriter parquetFileWriter;

    @Mock
    private FileStorage fileStorage;

    @Mock
    private FileNameGenerator fileNameGenerator;

    @Mock
    private HdfsProperties hdfsProperties;

    @Mock
    private GenericRecord genericRecord;

    @Mock
    private ParquetWriter<GenericRecord> writer;

    private Schema wireSchema;
    private Schema storageSchema;
    private NetFlowStorage storage;

    @BeforeEach
    void setUp() {
        lenient().when(hdfsProperties.getBasePath()).thenReturn("/netflow/parquet");
        lenient().when(fileNameGenerator.generateFileName(anyInt())).thenReturn("test.parquet");
        lenient().when(fileNameGenerator.generateTempFileName(anyString())).thenReturn(".test.parquet.tmp");

        wireSchema = new Schema.Parser().parse(
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

        storageSchema = buildStorageSchema(wireSchema);

        storage = new NetFlowStorage(
                batchPolicy,
                parquetFileWriter,
                fileStorage,
                fileNameGenerator,
                hdfsProperties,
                storageSchema
        );
    }

    private Schema buildStorageSchema(Schema wireSchema) {
        Schema.Field processingTimeField = new Schema.Field(
                "PROCESSING_TIME",
                Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG)),
                "Processing time",
                (Object) null
        );

        java.util.List<Schema.Field> fields = new java.util.ArrayList<>();
        for (Schema.Field field : wireSchema.getFields()) {
            Schema.Field copy = new Schema.Field(
                    field.name(),
                    field.schema(),
                    field.doc(),
                    field.defaultVal()
            );
            fields.add(copy);
        }
        fields.add(processingTimeField);

        return Schema.createRecord(
                wireSchema.getName(),
                wireSchema.getDoc(),
                wireSchema.getNamespace(),
                wireSchema.isError(),
                fields
        );
    }

    private IngestedRecord createRealRecord(int partition, long offset) {
        lenient().when(genericRecord.getSchema()).thenReturn(wireSchema);
        return new IngestedRecord(
                genericRecord,
                "test-topic",
                partition,
                offset,
                100
        );
    }

    private void setupWriterMocks() throws Exception {
        Path tempPath = new Path("/netflow/parquet", ".test.parquet.tmp");
        lenient().when(parquetFileWriter.createWriter(tempPath, storageSchema)).thenReturn(writer);
        lenient().doNothing().when(parquetFileWriter).write(any(ParquetWriter.class), any(GenericRecord.class));
        lenient().doNothing().when(parquetFileWriter).closeWriter(writer);
        lenient().doNothing().when(fileStorage).ensureDirectoryExists(any(Path.class));
        lenient().doNothing().when(fileStorage).commitAtomic(any(Path.class), any(Path.class));
        lenient().doNothing().when(fileStorage).cleanupTempFile(any(Path.class));
        lenient().when(genericRecord.getSchema()).thenReturn(wireSchema);
    }

    @Test
    @DisplayName("Should accept record and not flush when threshold not reached")
    void acceptShouldAddRecordAndNotFlushWhenThresholdNotReached() throws Exception {
        setupWriterMocks();

        IngestedRecord realRecord = createRealRecord(0, 100L);
        Optional<Map<TopicPartitionKey, Long>> result = storage.accept(realRecord);

        assertTrue(result.isEmpty());

        verify(fileStorage).ensureDirectoryExists(any(Path.class));
        verify(parquetFileWriter).createWriter(any(Path.class), eq(storageSchema));

        // Verify write was called with any GenericRecord (enriched record)
        ArgumentCaptor<GenericRecord> recordCaptor = ArgumentCaptor.forClass(GenericRecord.class);
        verify(parquetFileWriter).write(eq(writer), recordCaptor.capture());

        // Verify PROCESSING_TIME was added to the enriched record
        GenericRecord enriched = recordCaptor.getValue();
        assertNotNull(enriched.get("PROCESSING_TIME"), "PROCESSING_TIME should be present");
    }

    @Test
    @DisplayName("Should flush and commit when flush is called directly")
    void shouldFlushAndCommitWhenFlushIsCalled() throws Exception {
        setupWriterMocks();

        IngestedRecord realRecord = createRealRecord(0, 100L);

        storage.accept(realRecord);

        reset(parquetFileWriter, fileStorage);
        doNothing().when(parquetFileWriter).closeWriter(writer);
        doNothing().when(fileStorage).commitAtomic(any(Path.class), any(Path.class));

        Map<TopicPartitionKey, Long> result = storage.flush();

        assertNotNull(result);
        verify(parquetFileWriter).closeWriter(writer);
        verify(fileStorage).commitAtomic(any(Path.class), any(Path.class));
    }

    @Test
    @DisplayName("Should return empty map when accumulator is empty")
    void shouldReturnEmptyMapWhenAccumulatorIsEmpty() {
        Map<TopicPartitionKey, Long> result = storage.flush();
        assertTrue(result.isEmpty());
        verifyNoInteractions(parquetFileWriter, fileStorage);
    }

    @Test
    @DisplayName("Should cleanup temp file when write fails")
    void shouldCleanupTempFileWhenWriteFails() throws Exception {
        Path tempPath = new Path("/netflow/parquet", ".test.parquet.tmp");
        when(parquetFileWriter.createWriter(tempPath, storageSchema)).thenReturn(writer);
        doThrow(new ParquetWriteException("write failed"))
                .when(parquetFileWriter)
                .write(eq(writer), any(GenericRecord.class));
        doNothing().when(fileStorage).ensureDirectoryExists(any(Path.class));
        when(genericRecord.getSchema()).thenReturn(wireSchema);

        IngestedRecord realRecord = createRealRecord(0, 100L);

        assertThrows(StorageException.class, () -> storage.accept(realRecord));

        verify(fileStorage).cleanupTempFile(any(Path.class));
        verify(fileStorage, never()).commitAtomic(any(Path.class), any(Path.class));
    }

    @Test
    @DisplayName("Should reject null record")
    void shouldRejectNullRecord() {
        assertThrows(NullPointerException.class, () -> storage.accept(null));
        verifyNoInteractions(parquetFileWriter, fileStorage, fileNameGenerator);
    }

    @Test
    @DisplayName("Constructor should reject null dependencies")
    void constructorShouldRejectNullDependencies() {
        assertThrows(NullPointerException.class,
                () -> new NetFlowStorage(null, parquetFileWriter, fileStorage, fileNameGenerator, hdfsProperties, storageSchema));
        assertThrows(NullPointerException.class,
                () -> new NetFlowStorage(batchPolicy, null, fileStorage, fileNameGenerator, hdfsProperties, storageSchema));
        assertThrows(NullPointerException.class,
                () -> new NetFlowStorage(batchPolicy, parquetFileWriter, null, fileNameGenerator, hdfsProperties, storageSchema));
        assertThrows(NullPointerException.class,
                () -> new NetFlowStorage(batchPolicy, parquetFileWriter, fileStorage, null, hdfsProperties, storageSchema));
        assertThrows(NullPointerException.class,
                () -> new NetFlowStorage(batchPolicy, parquetFileWriter, fileStorage, fileNameGenerator, null, storageSchema));
        assertThrows(NullPointerException.class,
                () -> new NetFlowStorage(batchPolicy, parquetFileWriter, fileStorage, fileNameGenerator, hdfsProperties, null));
    }
}