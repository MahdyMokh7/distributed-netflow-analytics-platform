package com.nimbo.batch.integration;

import com.nimbo.batch.integration.config.TestFileSystemConfig;
import com.nimbo.batch.integration.config.TestParquetProperties;
import com.nimbo.batch.discovery.FileDiscoveryService;
import com.nimbo.batch.integration.util.TestDataGenerator;
import com.nimbo.batch.job.BatchJobRunner;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@SpringBootTest(properties = {
        "postgres.host=invalidhost",
        "postgres.port=5432",
        "output.mode=console"
})
@ActiveProfiles("test")
@Import(TestFileSystemConfig.class)
@EnableConfigurationProperties({TestParquetProperties.class})
public class BatchProcessorFallbackTest {

    @Autowired
    private BatchJobRunner batchJobRunner;

    @Autowired
    private SparkSession spark;

    @Autowired
    private FileDiscoveryService fileDiscoveryService;

    @Autowired
    private TestParquetProperties testProperties;

    @TempDir
    static java.nio.file.Path fallbackTempDir;

    @Test
    void testFallbackToInMemoryState_whenDBDown() {
        String hdfsUri = "file://" + fallbackTempDir.toString().replace('\\', '/');
        System.setProperty("hdfs.uri", hdfsUri);
        System.setProperty("hdfs.parquet-path", "/");

        Instant base = testProperties.getBaseTimestamp();
        int numFiles = testProperties.getFallbackFiles();
        int recordsPerFile = testProperties.getRecordsPerFile() / 2;
        TestDataGenerator.writeFiles(spark, new Path(fallbackTempDir.toString()), numFiles, recordsPerFile, base);

        batchJobRunner.runBatchJob();

        assertThat(fileDiscoveryService.isUsingFallback()).isTrue();
    }
}