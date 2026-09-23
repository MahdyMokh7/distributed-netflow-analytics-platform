package com.nimbo.storage.service;

import com.nimbo.ingester.model.IngestedRecord;
import com.nimbo.storage.config.HdfsProperties;
import com.nimbo.storage.config.ParquetProperties;
import com.nimbo.storage.model.TopicPartitionKey;
import com.nimbo.storage.service.batching.BatchPolicy;
import com.nimbo.storage.service.hdfs.FileStorage;
import com.nimbo.storage.service.hdfs.HdfsFileStorage;
import com.nimbo.storage.service.naming.FileNameGenerator;
import com.nimbo.storage.service.parquet.ParquetFileWriter;
import com.nimbo.metrics.BatchIngesterMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NetFlowStorageTest {

    @TempDir
    java.nio.file.Path tempDirectory;

    private FileSystem fileSystem;

    private BatchPolicy batchPolicy;
    private ParquetProperties parquetProperties;
    private ParquetFileWriter parquetFileWriter;
    private FileStorage fileStorage;
    private FileNameGenerator fileNameGenerator;
    private HdfsProperties hdfsProperties;

    private Schema sourceSchema;
    private Schema storageSchema;

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
                new Schema.Parser().parse(
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
                        """
                );

        storageSchema = buildStorageSchema(sourceSchema);

        meterRegistry = new SimpleMeterRegistry();
        metrics = new BatchIngesterMetrics(meterRegistry);
        metrics.registerInstance(1);

        storage = new NetFlowStorage(batchPolicy, parquetFileWriter, fileStorage, fileNameGenerator,
                hdfsProperties, storageSchema, metrics, 1);
    }

    @AfterEach
    void tearDown() throws IOException {
        fileSystem.close();
    }

    private IngestedRecord createRecord(int partition, long offset) {

        GenericRecord testRecord = new GenericData.Record(sourceSchema);
        testRecord.put("value", "test-value");

        return new IngestedRecord(testRecord, "test-topic", partition, offset, KAFKA_TIMESTAMP,100);
    }

    private Schema buildStorageSchema(Schema sourceSchema) {

        Schema.Field processTimeField =
                new Schema.Field(
                        "PROCESS_TIME",
                        Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG)),
                        "process time",
                        (Object) null
                );

        java.util.List<Schema.Field> fields = new java.util.ArrayList<>();

        for (Schema.Field field : sourceSchema.getFields()) {

            fields.add(new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal()));
        }

        fields.add(processTimeField);

        return Schema.createRecord(sourceSchema.getName(), sourceSchema.getDoc(), sourceSchema.getNamespace(),
                sourceSchema.isError(), fields);
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

            assertTrue(paths.anyMatch(path ->
                    path.toString().contains(expectedDate)
                            &&
                            path.toString().contains(expectedHour)
                            &&
                            path.toString().endsWith(".parquet")
            ));
        }
    }

    @Test
    void shouldNotCreateFilesWhenFlushingEmptyStorage() throws Exception {

        Map<TopicPartitionKey, Long> result = storage.flush();

        assertTrue(result.isEmpty());
        assertFalse(Files.exists(tempDirectory.resolve("parquet")));
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
                () -> new NetFlowStorage(null, parquetFileWriter, fileStorage, fileNameGenerator,
                        hdfsProperties, storageSchema, metrics, 1));

        assertThrows(
                NullPointerException.class,
                () -> new NetFlowStorage(batchPolicy, null, fileStorage, fileNameGenerator,
                        hdfsProperties, storageSchema, metrics, 1));

        assertThrows(
                NullPointerException.class,
                () -> new NetFlowStorage(batchPolicy, parquetFileWriter, null, fileNameGenerator,
                        hdfsProperties, storageSchema, metrics, 1));

        assertThrows(
                NullPointerException.class,
                () -> new NetFlowStorage(batchPolicy, parquetFileWriter, fileStorage, null,
                        hdfsProperties, storageSchema, metrics, 1));

        assertThrows(
                NullPointerException.class,
                () -> new NetFlowStorage(batchPolicy, parquetFileWriter, fileStorage, fileNameGenerator,
                        null, storageSchema, metrics, 1));

        assertThrows(
                NullPointerException.class,
                () -> new NetFlowStorage(batchPolicy, parquetFileWriter, fileStorage, fileNameGenerator,
                        hdfsProperties, null, metrics, 1));

        assertThrows(
                NullPointerException.class,
                () -> new NetFlowStorage(batchPolicy, parquetFileWriter, fileStorage, fileNameGenerator,
                        hdfsProperties, storageSchema, null, 1));
    }
}
