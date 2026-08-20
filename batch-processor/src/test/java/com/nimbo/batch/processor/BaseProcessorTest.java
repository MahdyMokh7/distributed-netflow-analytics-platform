package com.nimbo.batch.processor;

import com.nimbo.batch.config.SparkConfig;
import com.nimbo.batch.config.TestParquetConfig;
import com.nimbo.batch.util.TestDataGenerator;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

@SpringBootTest(classes = {TestParquetConfig.class, SparkConfig.class})
@EnableConfigurationProperties({TestParquetConfig.class, SparkConfig.class})
@ActiveProfiles("test")
@ExtendWith(SpringExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
abstract class BaseProcessorTest {

    protected SparkSession spark;

    @Autowired
    protected TestParquetConfig testParquetConfig;

    @Autowired
    protected SparkConfig sparkConfig;

    @BeforeAll
    void setUpSparkAndFixtures() throws IOException {
        spark = sparkConfig.sparkSession();

        generateFixtures();
    }

    @AfterAll
    void tearDownSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    private void generateFixtures() throws IOException {
        Path samplePath = Path.of(testParquetConfig.getLocalPath());
        deleteRecursively(samplePath);
        Files.createDirectories(samplePath);

        TestDataGenerator.writeSampleParquetFiles(
                spark,
                testParquetConfig.getLocalPath(),
                testParquetConfig.getRecordsPerFile(),
                testParquetConfig.getFilesPerPartition());

        // Deliberately empty directory: no parquet files written here.
        Path emptyPath = Path.of(testParquetConfig.getEmptyPath());
        deleteRecursively(emptyPath);
        Files.createDirectories(emptyPath);
    }

    private void deleteRecursively(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        try (var stream = Files.walk(path)) {
            stream.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
    }
}