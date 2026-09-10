package com.nimbo.batch.job;

import com.nimbo.batch.config.BatchConfig;
import com.nimbo.batch.config.HdfsConfig;
import com.nimbo.batch.config.OutputConfig;
import com.nimbo.batch.dao.BatchStateDao;
import com.nimbo.batch.discovery.FileDiscoveryService;
import com.nimbo.batch.metrics.BatchProcessorMetrics;
import com.nimbo.batch.processor.BaseProcessor;
import com.nimbo.batch.processor.ConnectionProcessor;
import com.nimbo.batch.processor.ProtocolProcessor;
import com.nimbo.batch.processor.TopIpProcessor;
import com.nimbo.batch.processor.TrafficAggregationProcessor;
import com.nimbo.batch.support.PostgresContainerExtension;
import com.nimbo.batch.support.SparkSessionExtension;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.jdbi.v3.core.Jdbi;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class BatchJobRunnerIT {

    private static final StructType NETFLOW_SCHEMA = new StructType()
            .add("IPV4_SRC_ADDR", DataTypes.StringType, false)
            .add("IPV4_DST_ADDR", DataTypes.StringType, false)
            .add("L4_SRC_PORT", DataTypes.IntegerType, false)
            .add("L4_DST_PORT", DataTypes.IntegerType, false)
            .add("PROTOCOL", DataTypes.IntegerType, false)
            .add("L7_PROTO", DataTypes.DoubleType, false)
            .add("IN_BYTES", DataTypes.LongType, false)
            .add("OUT_BYTES", DataTypes.LongType, false)
            .add("IN_PKTS", DataTypes.LongType, false)
            .add("OUT_PKTS", DataTypes.LongType, false)
            .add("PROCESSING_TIME", DataTypes.LongType, false);

    @RegisterExtension
    static final PostgresContainerExtension POSTGRES = new PostgresContainerExtension();

    @RegisterExtension
    static final SparkSessionExtension SPARK = new SparkSessionExtension();

    @TempDir
    java.nio.file.Path directory;

    private SparkSession spark;
    private FileSystem fileSystem;
    private BatchStateDao stateDao;
    private BatchConfig batchConfig;
    private FileDiscoveryService fileDiscoveryService;
    private BatchProcessorMetrics metrics;
    private SimpleMeterRegistry meterRegistry;

    @BeforeEach
    void setUp() throws IOException {
        spark = SPARK.spark();
        fileSystem = FileSystem.getLocal(new Configuration());
        Jdbi jdbi = POSTGRES.jdbi();
        jdbi.useHandle(handle -> handle.execute("TRUNCATE batch_processor_state"));
        stateDao = jdbi.onDemand(BatchStateDao.class);
        batchConfig = new BatchConfig();
        batchConfig.setEnabled(true);
        batchConfig.setWindowDuration("1 hour");
        batchConfig.setMaxFilesPerBatch(5);
        HdfsConfig hdfsConfig = new HdfsConfig();
        hdfsConfig.setUri("file://");
        hdfsConfig.setParquetPath(directory.toString());
        meterRegistry = new SimpleMeterRegistry();
        metrics = new BatchProcessorMetrics(meterRegistry);
        fileDiscoveryService = new FileDiscoveryService(fileSystem, hdfsConfig, batchConfig, stateDao, metrics);
    }

    @AfterEach
    void closeFileSystem() throws IOException {
        fileSystem.close();
    }

    @Test
    void givenAnEnabledJobAndANetflowFile_whenRun_thenItProcessesAndRecordsTheFile() throws IOException {
        // Given: one real Parquet file and all four real processors
        String fileName = "netflow-p01-2026-08-29T08-30-00-000Z-job.parquet";
        writeNetflowFile(fileName);
        BatchJobRunner runner = runner();

        // When: the scheduled job entry point is invoked directly
        runner.runBatchJob();

        // Then: the file is durably recorded after all processor paths complete
        assertThat(stateDao.getProcessedFileNames()).containsExactly(fileName);
        assertThat(fileDiscoveryService.getFailedBatchId()).isNull();
        assertThat(meterRegistry.get("batch.processor.runs").tag("outcome", "success").counter().count())
                .isEqualTo(1.0);
        assertThat(meterRegistry.get("batch.processor.source.records").counter().count()).isEqualTo(2.0);
        assertThat(meterRegistry.get("batch.processor.run.in.progress").gauge().value()).isZero();
    }

    @Test
    void givenAnEnabledJobWithoutFiles_whenRun_thenItCompletesWithoutStateChanges() {
        // Given: an enabled job with an empty discovery directory
        BatchJobRunner runner = runner();

        // When: the scheduled job entry point is invoked directly
        runner.runBatchJob();

        // Then: no file state is created
        assertThat(stateDao.getProcessedFileNames()).isEmpty();
        assertThat(meterRegistry.get("batch.processor.runs").tag("outcome", "no_data").counter().count())
                .isEqualTo(1.0);
    }

    @Test
    void givenADisabledJobAndAFile_whenRun_thenItLeavesTheFileUnprocessed() throws IOException {
        // Given: a discoverable file and a disabled batch configuration
        String fileName = "netflow-p01-2026-08-29T08-30-00-000Z-disabled.parquet";
        writeNetflowFile(fileName);
        batchConfig.setEnabled(false);
        BatchJobRunner runner = runner();

        // When: the scheduled job entry point is invoked directly
        runner.runBatchJob();

        // Then: the job does not record the available file
        assertThat(stateDao.getProcessedFileNames()).isEmpty();
        assertThat(meterRegistry.get("batch.processor.runs").tag("outcome", "disabled").counter().count())
                .isEqualTo(1.0);
    }

    private BatchJobRunner runner() {
        OutputConfig outputConfig = new OutputConfig();
        outputConfig.setMode(OutputConfig.Mode.NONE);
        List<BaseProcessor> processors = List.of(
                new TrafficAggregationProcessor(batchConfig, outputConfig, null),
                new ConnectionProcessor(batchConfig, outputConfig, null),
                new TopIpProcessor(batchConfig, outputConfig, null),
                new ProtocolProcessor(batchConfig, outputConfig, null));
        return new BatchJobRunner(processors, batchConfig, outputConfig, fileDiscoveryService, spark, metrics);
    }

    private void writeNetflowFile(String fileName) throws IOException {
        java.nio.file.Path staging = directory.resolve("staging-" + fileName);
        java.nio.file.Path hourDirectory = Files.createDirectories(directory.resolve("2026-08-29").resolve("08"));
        long timestampMicros = java.time.Instant.parse("2026-08-29T08:30:00Z").toEpochMilli() * 1_000;
        List<Row> rows = List.of(
                RowFactory.create("10.0.0.1", "10.0.0.2", 12345, 443, 6, 6.0, 100L, 200L, 1L, 2L, timestampMicros),
                RowFactory.create("10.0.0.1", "10.0.0.2", 54321, 8443, 17, 6.0, 300L, 400L, 3L, 4L, timestampMicros));
        spark.createDataFrame(rows, NETFLOW_SCHEMA).coalesce(1).write().parquet(staging.toString());
        try (var paths = Files.list(staging)) {
            java.nio.file.Path partFile = paths
                    .filter(path -> path.getFileName().toString().startsWith("part-"))
                    .findFirst()
                    .orElseThrow();
            Files.move(partFile, hourDirectory.resolve(fileName), StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
