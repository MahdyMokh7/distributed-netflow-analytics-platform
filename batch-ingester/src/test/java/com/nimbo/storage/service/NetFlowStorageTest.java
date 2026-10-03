package com.nimbo.storage.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.nimbo.ingester.model.IngestedRecord;
import com.nimbo.metrics.BatchIngesterMetrics;
import com.nimbo.storage.config.HdfsProperties;
import com.nimbo.storage.config.ParquetProperties;
import com.nimbo.storage.exception.StorageException;
import com.nimbo.storage.model.TopicPartitionKey;
import com.nimbo.storage.service.batching.BatchPolicy;
import com.nimbo.storage.service.hdfs.FileStorage;
import com.nimbo.storage.service.hdfs.HdfsFileStorage;
import com.nimbo.storage.service.naming.FileNameGenerator;
import com.nimbo.storage.service.parquet.ParquetFileWriter;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

class NetFlowStorageTest {

    @TempDir java.nio.file.Path tempDirectory;

    private FileSystem fileSystem;

    private BatchPolicy batchPolicy;
    private ParquetProperties parquetProperties;
    private ParquetFileWriter parquetFileWriter;
    private FileStorage fileStorage;
    private FileNameGenerator fileNameGenerator;
    private HdfsProperties hdfsProperties;

    private Schema sourceSchema;

    private NetFlowStorage storage;

    private BatchIngesterMetrics metrics;
    private SimpleMeterRegistry meterRegistry;

    private static final long KAFKA_TIMESTAMP = 1_700_000_000_000L;

    @BeforeEach
    void setUp() throws IOException {

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "file:///");

        fileSystem = FileSystem.newInstance(URI.create("file:///"), configuration);
        parquetProperties = new ParquetProperties();
        parquetProperties.setCompressionCodec("SNAPPY");
        parquetProperties.setBlockSizeBytes(1024 * 1024);
        parquetProperties.setPageSizeBytes(1024 * 1024);
        parquetProperties.setEnableDictionary(true);
        parquetFileWriter = new ParquetFileWriter(fileSystem, parquetProperties);
        fileStorage = new HdfsFileStorage(fileSystem);
        fileNameGenerator = new FileNameGenerator();
        hdfsProperties = new HdfsProperties();
        hdfsProperties.setBasePath(tempDirectory.resolve("parquet").toUri().toString());
        batchPolicy = new BatchPolicy(100, 1_000_000, Duration.ofMinutes(5));

        sourceSchema =
                new Schema.Parser()
                        .parse(
                                """
                        {
                          "type": "record",
                          "name": "TestRecord",
                          "fields": [
                            {
                              "name": "value",
                              "type": "string"
                            }
                          ]
                        }
                        """);

        meterRegistry = new SimpleMeterRegistry();
        metrics = new BatchIngesterMetrics(meterRegistry);
        metrics.registerInstance(1);

        storage =
                new NetFlowStorage(
                        batchPolicy,
                        parquetFileWriter,
                        fileStorage,
                        fileNameGenerator,
                        hdfsProperties,
                        metrics,
                        1,
                        List.of(sourceSchema));
    }

    @AfterEach
    void tearDown() throws IOException {
        fileSystem.close();
    }

    private IngestedRecord createRecord(int partition, long offset) {

        GenericRecord testRecord = new GenericData.Record(sourceSchema);
        testRecord.put("value", "test-value");

        return new IngestedRecord(
                testRecord, "test-topic", partition, offset, KAFKA_TIMESTAMP, 100);
    }

    private IngestedRecord createRecord(GenericRecord record, long offset) {
        return new IngestedRecord(record, "test-topic", 0, offset, KAFKA_TIMESTAMP, 100);
    }

    @Test
    @DisplayName("Should accept record and not flush when threshold not reached")
    void shouldAcceptRecordWithoutFlushingUntilThresholdReached() throws Exception {

        IngestedRecord testRecord = createRecord(0, 100L);
        Optional<Map<TopicPartitionKey, Long>> result = storage.accept(testRecord);

        assertTrue(result.isEmpty());
    }

    @Test
    @DisplayName("Should flush and create parquet file using Kafka timestamp path")
    void shouldFlushAndCreateKafkaTimestampBasedPath() throws Exception {

        IngestedRecord realRecord = createRecord(0, 100L);
        storage.accept(realRecord);
        storage.flush();
        String expectedDate = "2023-11-14"; // 1_700_000_000_000L UTC date

        String expectedHour = "22";

        try (var paths = Files.walk(tempDirectory.resolve("parquet"))) {

            assertTrue(
                    paths.anyMatch(
                            path ->
                                    path.toString().contains(expectedDate)
                                            && path.toString().contains(expectedHour)
                                            && path.toString().endsWith(".parquet")));
        }
    }

    @Test
    @DisplayName("Should persist Kafka metadata in Parquet")
    void shouldPersistKafkaMetadata() throws Exception {
        storage.accept(createRecord(7, 123L));
        storage.flush();

        GenericRecord persisted = readAllParquetRecords().get(0);
        assertEquals(KAFKA_TIMESTAMP * 1000, persisted.get("PROCESS_TIME"));
        assertEquals("test-topic", persisted.get("KAFKA_TOPIC").toString());
        assertEquals(7, persisted.get("KAFKA_PARTITION"));
        assertEquals(123L, persisted.get("KAFKA_OFFSET"));
    }

    @Test
    void shouldNotCreateFilesWhenFlushingEmptyStorage() throws Exception {

        Map<TopicPartitionKey, Long> result = storage.flush();

        assertTrue(result.isEmpty());
        assertFalse(Files.exists(tempDirectory.resolve("parquet")));
    }

    @Test
    @DisplayName("Should expand the Parquet schema once when old and new records are interleaved")
    void shouldExpandWriterSchemaOnceWhenVersionsAreInterleaved() throws Exception {
        Schema evolvedSchema =
                new Schema.Parser()
                        .parse(
                                """
                {
                  "type": "record",
                  "name": "TestRecord",
                  "fields": [
                    {"name": "value", "type": "string"},
                    {"name": "new_field", "type": ["null", "string"], "default": null}
                  ]
                }
                """);

        GenericRecord originalRecord = new GenericData.Record(sourceSchema);
        originalRecord.put("value", "v1");
        GenericRecord evolvedRecord = new GenericData.Record(evolvedSchema);
        evolvedRecord.put("value", "v2");
        evolvedRecord.put("new_field", "preserved");

        storage.accept(createRecord(originalRecord, 100L));
        Optional<Map<TopicPartitionKey, Long>> rotatedOffsets =
                storage.accept(createRecord(evolvedRecord, 101L));
        GenericRecord laterOriginalRecord = new GenericData.Record(sourceSchema);
        laterOriginalRecord.put("value", "v1-again");
        Optional<Map<TopicPartitionKey, Long>> repeatedVersionOffsets =
                storage.accept(createRecord(laterOriginalRecord, 102L));
        Map<TopicPartitionKey, Long> finalOffsets = storage.flush();

        TopicPartitionKey partition = new TopicPartitionKey("test-topic", 0);
        assertEquals(101L, rotatedOffsets.orElseThrow().get(partition));
        assertTrue(repeatedVersionOffsets.isEmpty());
        assertEquals(103L, finalOffsets.get(partition));

        List<GenericRecord> persistedRecords = readAllParquetRecords();
        assertEquals(3, persistedRecords.size());
        assertEquals(2, parquetFileCount());

        GenericRecord persistedV1 =
                persistedRecords.stream()
                        .filter(record -> record.get("value").toString().equals("v1"))
                        .findFirst()
                        .orElseThrow();
        GenericRecord persistedV2 =
                persistedRecords.stream()
                        .filter(record -> record.get("value").toString().equals("v2"))
                        .findFirst()
                        .orElseThrow();
        GenericRecord persistedLaterV1 =
                persistedRecords.stream()
                        .filter(record -> record.get("value").toString().equals("v1-again"))
                        .findFirst()
                        .orElseThrow();

        assertNull(persistedV1.getSchema().getField("new_field"));
        assertEquals("v1", persistedV1.get("value").toString());
        assertEquals("v2", persistedV2.get("value").toString());
        assertEquals("preserved", persistedV2.get("new_field").toString());
        assertNull(persistedLaterV1.get("new_field"));
        assertEquals(KAFKA_TIMESTAMP * 1000, persistedV1.get("PROCESS_TIME"));
        assertEquals(KAFKA_TIMESTAMP * 1000, persistedV2.get("PROCESS_TIME"));
        assertEquals(KAFKA_TIMESTAMP * 1000, persistedLaterV1.get("PROCESS_TIME"));
    }

    @Test
    @DisplayName("Should keep the registered storage schema after a restart")
    void shouldKeepRegisteredStorageSchemaAfterRestart() throws Exception {
        Schema evolvedSchema =
                new Schema.Parser()
                        .parse(
                                """
                {
                  "type": "record",
                  "name": "TestRecord",
                  "fields": [
                    {"name": "value", "type": "string"},
                    {"name": "new_field", "type": ["null", "string"], "default": null}
                  ]
                }
                """);
        GenericRecord originalRecord = new GenericData.Record(sourceSchema);
        originalRecord.put("value", "v1");
        List<Schema> registeredSchemas = List.of(sourceSchema, evolvedSchema);

        metrics.registerInstance(2);
        NetFlowStorage beforeRestart =
                new NetFlowStorage(
                        batchPolicy,
                        parquetFileWriter,
                        fileStorage,
                        fileNameGenerator,
                        hdfsProperties,
                        metrics,
                        2,
                        registeredSchemas);
        beforeRestart.accept(createRecord(originalRecord, 100L));
        beforeRestart.flush();

        metrics.registerInstance(3);
        NetFlowStorage afterRestart =
                new NetFlowStorage(
                        batchPolicy,
                        parquetFileWriter,
                        fileStorage,
                        fileNameGenerator,
                        hdfsProperties,
                        metrics,
                        3,
                        registeredSchemas);
        afterRestart.accept(createRecord(originalRecord, 101L));
        afterRestart.flush();

        List<GenericRecord> records = readAllParquetRecords();
        assertEquals(2, records.size());
        assertEquals(records.get(0).getSchema(), records.get(1).getSchema());
        assertTrue(records.get(0).getSchema().getField("new_field").schema().isNullable());
        assertNull(records.get(0).get("new_field"));
        assertNull(records.get(1).get("new_field"));
    }

    @Test
    @DisplayName(
            "Should retain a removed column and write null for records that no longer contain it")
    void shouldRetainRemovedColumnInStorageSchema() throws Exception {
        Schema schemaWithoutValue =
                new Schema.Parser()
                        .parse(
                                """
                {
                  "type": "record",
                  "name": "TestRecord",
                  "fields": []
                }
                """);
        GenericRecord originalRecord = new GenericData.Record(sourceSchema);
        originalRecord.put("value", "before-removal");
        GenericRecord recordWithoutValue = new GenericData.Record(schemaWithoutValue);
        GenericRecord laterOriginalRecord = new GenericData.Record(sourceSchema);
        laterOriginalRecord.put("value", "old-version-again");

        storage.accept(createRecord(originalRecord, 100L));
        storage.accept(createRecord(recordWithoutValue, 101L));
        Optional<Map<TopicPartitionKey, Long>> repeatedVersionOffsets =
                storage.accept(createRecord(laterOriginalRecord, 102L));
        storage.flush();

        assertTrue(repeatedVersionOffsets.isEmpty());
        assertEquals(2, parquetFileCount());
        List<GenericRecord> persistedRecords = readAllParquetRecords();
        GenericRecord persistedWithoutValue =
                persistedRecords.stream()
                        .filter(record -> record.get("value") == null)
                        .findFirst()
                        .orElseThrow();
        GenericRecord persistedLaterV1 =
                persistedRecords.stream()
                        .filter(
                                record ->
                                        record.get("value") != null
                                                && record.get("value")
                                                        .toString()
                                                        .equals("old-version-again"))
                        .findFirst()
                        .orElseThrow();

        assertNull(persistedWithoutValue.get("value"));
        assertEquals("old-version-again", persistedLaterV1.get("value").toString());
    }

    @Test
    @DisplayName("Should merge an unseen schema again when the first rotation fails")
    void shouldNotCacheSchemaBeforeRotationSucceeds() throws Exception {
        metrics.registerInstance(2);
        Schema evolvedSchema =
                new Schema.Parser()
                        .parse(
                                """
                {
                  "type": "record",
                  "name": "TestRecord",
                  "fields": [
                    {"name": "value", "type": "string"},
                    {"name": "new_field", "type": ["null", "string"], "default": null}
                  ]
                }
                """);
        GenericRecord originalRecord = new GenericData.Record(sourceSchema);
        originalRecord.put("value", "v1");
        GenericRecord evolvedRecord = new GenericData.Record(evolvedSchema);
        evolvedRecord.put("value", "v2");
        evolvedRecord.put("new_field", "preserved");

        NetFlowStorage retryingStorage =
                new NetFlowStorage(
                        batchPolicy,
                        parquetFileWriter,
                        fileStorage,
                        fileNameGenerator,
                        hdfsProperties,
                        metrics,
                        2,
                        List.of(sourceSchema));

        retryingStorage.accept(createRecord(originalRecord, 100L));
        java.nio.file.Path temporaryFile = onlyTemporaryParquetFile();
        java.nio.file.Path finalPath = finalPathFor(temporaryFile);
        Files.createFile(finalPath);

        assertThrows(
                StorageException.class,
                () -> retryingStorage.accept(createRecord(evolvedRecord, 101L)));

        Files.delete(finalPath);

        retryingStorage.accept(createRecord(evolvedRecord, 101L));
        retryingStorage.flush();

        List<GenericRecord> records = readAllParquetRecords();
        assertEquals(1, records.size());
        assertEquals("preserved", records.get(0).get("new_field").toString());
        assertTrue(records.get(0).getSchema().getField("new_field").schema().isNullable());
    }

    @Test
    @DisplayName("Should reject null record")
    void shouldRejectNullRecord() {
        assertThrows(NullPointerException.class, () -> storage.accept(null));
    }

    @Test
    @DisplayName("Constructor should reject null dependencies")
    void shouldRejectNullDependencies() {
        assertThrows(
                NullPointerException.class,
                () ->
                        new NetFlowStorage(
                                null,
                                parquetFileWriter,
                                fileStorage,
                                fileNameGenerator,
                                hdfsProperties,
                                metrics,
                                1,
                                List.of(sourceSchema)));

        assertThrows(
                NullPointerException.class,
                () ->
                        new NetFlowStorage(
                                batchPolicy,
                                null,
                                fileStorage,
                                fileNameGenerator,
                                hdfsProperties,
                                metrics,
                                1,
                                List.of(sourceSchema)));

        assertThrows(
                NullPointerException.class,
                () ->
                        new NetFlowStorage(
                                batchPolicy,
                                parquetFileWriter,
                                null,
                                fileNameGenerator,
                                hdfsProperties,
                                metrics,
                                1,
                                List.of(sourceSchema)));

        assertThrows(
                NullPointerException.class,
                () ->
                        new NetFlowStorage(
                                batchPolicy,
                                parquetFileWriter,
                                fileStorage,
                                null,
                                hdfsProperties,
                                metrics,
                                1,
                                List.of(sourceSchema)));

        assertThrows(
                NullPointerException.class,
                () ->
                        new NetFlowStorage(
                                batchPolicy,
                                parquetFileWriter,
                                fileStorage,
                                fileNameGenerator,
                                null,
                                metrics,
                                1,
                                List.of(sourceSchema)));

        assertThrows(
                NullPointerException.class,
                () ->
                        new NetFlowStorage(
                                batchPolicy,
                                parquetFileWriter,
                                fileStorage,
                                fileNameGenerator,
                                hdfsProperties,
                                null,
                                1,
                                List.of(sourceSchema)));

        assertThrows(
                NullPointerException.class,
                () ->
                        new NetFlowStorage(
                                batchPolicy,
                                parquetFileWriter,
                                fileStorage,
                                fileNameGenerator,
                                hdfsProperties,
                                metrics,
                                1,
                                null));
    }

    private List<GenericRecord> readAllParquetRecords() throws Exception {
        List<GenericRecord> records = new ArrayList<>();
        var files =
                fileSystem.listFiles(
                        new org.apache.hadoop.fs.Path(hdfsProperties.getBasePath()), true);
        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            if (!file.getPath().getName().endsWith(".parquet")) {
                continue;
            }
            try (ParquetReader<GenericRecord> reader =
                    AvroParquetReader.<GenericRecord>builder(
                                    HadoopInputFile.fromPath(file.getPath(), fileSystem.getConf()))
                            .withDataModel(GenericData.get())
                            .build()) {
                GenericRecord record;
                while ((record = reader.read()) != null) {
                    records.add(record);
                }
            }
        }
        return records;
    }

    private long parquetFileCount() throws IOException {
        try (var paths = Files.walk(tempDirectory.resolve("parquet"))) {
            return paths.filter(path -> path.toString().endsWith(".parquet")).count();
        }
    }

    private java.nio.file.Path onlyTemporaryParquetFile() {
        try (var paths = Files.walk(tempDirectory.resolve("parquet"))) {
            return paths.filter(path -> path.getFileName().toString().endsWith(".parquet.tmp"))
                    .findFirst()
                    .orElseThrow();
        } catch (IOException exception) {
            throw new IllegalStateException("Failed locating temporary Parquet file", exception);
        }
    }

    private java.nio.file.Path finalPathFor(java.nio.file.Path temporaryFile) {
        String temporaryName = temporaryFile.getFileName().toString();
        String finalName = temporaryName.substring(1, temporaryName.length() - ".tmp".length());
        return temporaryFile.resolveSibling(finalName);
    }
}
