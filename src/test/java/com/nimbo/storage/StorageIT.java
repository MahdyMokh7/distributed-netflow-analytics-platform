package com.nimbo.storage;

import com.nimbo.ingester.model.IngestedRecord;
import com.nimbo.storage.config.HdfsProperties;
import com.nimbo.storage.config.ParquetProperties;
import com.nimbo.storage.model.TopicPartitionKey;
import com.nimbo.storage.service.NetFlowStorage;
import com.nimbo.storage.service.batching.BatchAccumulator;
import com.nimbo.storage.service.batching.BatchPolicy;
import com.nimbo.storage.service.hdfs.HdfsFileStorage;
import com.nimbo.storage.service.naming.FileNameGenerator;
import com.nimbo.storage.service.parquet.ParquetFileWriter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class StorageIT {

    @Container
    private static final GenericContainer<?> HDFS_CONTAINER =
            new GenericContainer<>(
                    DockerImageName.parse("apache/hadoop:3.3.6")
            )
                    .withCopyFileToContainer(
                            MountableFile.forClasspathResource(
                                    "hadoop/core-site.xml"
                            ),
                            "/opt/hadoop/etc/hadoop/core-site.xml"
                    )
                    .withCopyFileToContainer(
                            MountableFile.forClasspathResource(
                                    "hadoop/hdfs-site.xml"
                            ),
                            "/opt/hadoop/etc/hadoop/hdfs-site.xml"
                    )
                    .withExposedPorts(
                            8020,
                            9870,
                            9866
                    )
                    .withCopyFileToContainer(
                            MountableFile.forClasspathResource("hadoop/start-hdfs.sh"),
                            "/opt/hadoop/start-hdfs.sh"
                    )
                    .withCommand("bash", "/opt/hadoop/start-hdfs.sh")
                    .waitingFor(
                            Wait.forLogMessage(
                                            ".*HDFS_READY.*",
                                            1
                                    )
                                    .withStartupTimeout(
                                            Duration.ofMinutes(2)
                                    )
                    );

    private static FileSystem fileSystem;
    private static NetFlowStorage netFlowStorage;
    private static Configuration hadoopConfiguration;
    private static Schema avroSchema;

    private static final List<GenericRecord> ingestedRecords =
            new ArrayList<>();

    @BeforeAll
    static void setUpAll() throws Exception {
        avroSchema = createAvroSchema();
        hadoopConfiguration = createHadoopConfiguration();
        fileSystem = createFileSystem(hadoopConfiguration);

        netFlowStorage = createStorageFixture(fileSystem);

        ingestedRecords.clear();
    }


    private static Schema createAvroSchema() throws Exception {
        String schemaJson = """
                {
                  "type": "record",
                  "name": "NetFlowRecord",
                  "fields": [
                    {
                      "name": "srcIp",
                      "type": "string"
                    },
                    {
                      "name": "dstIp",
                      "type": "string"
                    },
                    {
                      "name": "bytes",
                      "type": "long"
                    }
                  ]
                }
                """;
        return new Schema.Parser().parse(schemaJson);
    }

    private static Configuration createHadoopConfiguration() {
        String hdfsUri =
                "hdfs://"
                        + HDFS_CONTAINER.getHost()
                        + ":"
                        + HDFS_CONTAINER.getMappedPort(8020);

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUri);
        conf.set(
                "fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        // The container's hdfs-site.xml is not loaded by the client,
        // so set replication explicitly for the test JVM.
        conf.set("dfs.replication", "1");
        return conf;
    }

    private static FileSystem createFileSystem(Configuration conf)
            throws IOException, InterruptedException {
        String hdfsUri = conf.get("fs.defaultFS");
        FileSystem fs = FileSystem.get(
                URI.create(hdfsUri),
                conf,
                "hadoop"
        );
        assertTrue(
                fs.exists(new Path("/")),
                "HDFS root should be accessible"
        );
        return fs;
    }

    // ---------- Storage fixture helpers ----------

    private static NetFlowStorage createStorageFixture(FileSystem fs) {
        BatchPolicy batchPolicy = new BatchPolicy(
                5,
                1024 * 1024,
                Duration.ofMinutes(2)
        );
        BatchAccumulator batchAccumulator = new BatchAccumulator(batchPolicy);

        ParquetProperties parquetProperties = new ParquetProperties();
        parquetProperties.setCompressionCodec("SNAPPY");
        parquetProperties.setBlockSizeBytes(128 * 1024 * 1024);
        parquetProperties.setPageSizeBytes(1024 * 1024);
        parquetProperties.setEnableDictionary(true);

        ParquetFileWriter parquetFileWriter =
                new ParquetFileWriter(fs, parquetProperties);

        FileNameGenerator fileNameGenerator = new FileNameGenerator();
        HdfsFileStorage hdfsFileStorage = new HdfsFileStorage(fs);

        HdfsProperties hdfsProperties = new HdfsProperties();
        hdfsProperties.setBasePath("/netflow/parquet");

        return new NetFlowStorage(
                batchAccumulator,
                parquetFileWriter,
                hdfsFileStorage,
                fileNameGenerator,
                hdfsProperties
        );
    }

    @Test
    @DisplayName(
            "persisting a batch as Parquet in HDFS and reading it back."
    )
    void shouldWriteAndReadParquetThroughHdfs() throws Exception {

        /*
         * Record 1:
         * verify that the batch is initially buffered.
         */
        GenericRecord firstRecord =
                createRecord(
                        "192.168.1.10",
                        "10.0.0.1",
                        500L
                );

        ingestedRecords.add(firstRecord);

        Optional<Map<TopicPartitionKey, Long>> result =
                netFlowStorage.accept(
                        createIngestedRecord(
                                firstRecord,
                                100L
                        )
                );

        assertTrue(
                result.isEmpty(),
                "The first record must remain buffered"
        );

        /*
         * Records 2-5.
         * The fifth record reaches the batch threshold.
         */
        for (int i = 2; i <= 5; i++) {

            GenericRecord record =
                    createRecord(
                            "192.168.1." + i,
                            "10.0.0.1",
                            1000L * i
                    );

            ingestedRecords.add(record);

            result =
                    netFlowStorage.accept(
                            createIngestedRecord(
                                    record,
                                    99L + i
                            )
                    );
        }

        /*
         * The fifth record must trigger persistence.
         */
        assertTrue(
                result.isPresent(),
                "The fifth record must trigger a batch flush"
        );

        assertEquals(
                105L,
                result.get().get(
                        new TopicPartitionKey(
                                "netflow",
                                0
                        )
                )
        );

        /*
         * Verify the HDFS directory exists.
         */
        Path storageDirectory =
                new Path("/netflow/parquet");

        assertTrue(
                fileSystem.exists(storageDirectory),
                "The HDFS target directory must exist"
        );

        /*
         * Verify that a real Parquet file was committed.
         */
        FileStatus[] files =
                fileSystem.listStatus(
                        storageDirectory
                );

        assertEquals(
                1,
                files.length,
                "Exactly one Parquet file should have been created"
        );

        Path parquetFile =
                files[0].getPath();

        assertTrue(
                parquetFile.getName().endsWith(".parquet"),
                "The committed file must have a .parquet extension"
        );

        assertFalse(
                parquetFile.getName().startsWith("."),
                "The temporary file must not remain as the final file"
        );

        assertTrue(
                files[0].getLen() > 0,
                "The Parquet file must not be empty"
        );

        /*
         * Read the actual Parquet file back from HDFS.
         *
         * IMPORTANT:
         * Reuse the HDFS configuration. A new empty
         * Configuration would default to the local filesystem.
         */
        try (ParquetReader<GenericRecord> reader =
                     AvroParquetReader
                             .<GenericRecord>builder(
                                     HadoopInputFile.fromPath(
                                             parquetFile,
                                             hadoopConfiguration
                                     )
                             )
                             .build()) {

            int count = 0;

            GenericRecord actual;

            while ((actual = reader.read()) != null) {

                assertTrue(
                        count < ingestedRecords.size(),
                        "HDFS contains more records than expected"
                );

                GenericRecord expected =
                        ingestedRecords.get(count);

                assertEquals(
                        expected.get("srcIp").toString(),
                        actual.get("srcIp").toString()
                );

                assertEquals(
                        expected.get("dstIp").toString(),
                        actual.get("dstIp").toString()
                );

                assertEquals(
                        expected.get("bytes"),
                        actual.get("bytes")
                );

                count++;
            }

            assertEquals(
                    ingestedRecords.size(),
                    count,
                    "All ingested records must be readable from Parquet"
            );
        }
    }

    private static GenericRecord createRecord(
            String srcIp,
            String dstIp,
            long bytes
    ) {

        GenericRecord record =
                new GenericData.Record(avroSchema);

        record.put("srcIp", srcIp);
        record.put("dstIp", dstIp);
        record.put("bytes", bytes);

        return record;
    }

    private static IngestedRecord createIngestedRecord(
            GenericRecord record,
            long offset
    ) {

        return new IngestedRecord(
                record,
                "netflow",
                0,
                offset,
                100
        );
    }

    @AfterAll
    static void tearDownAll() throws IOException {

        if (fileSystem != null) {
            fileSystem.close();
        }
    }
}