package com.nimbo.batch.integration;

import com.nimbo.batch.config.HdfsConfig;
import com.nimbo.batch.config.SparkConfig;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(classes = {HdfsConfig.class, SparkConfig.class})
@EnableConfigurationProperties({HdfsConfig.class, SparkConfig.class})
@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public abstract class HdfsIntegrationTestBase {

    protected static SparkSession spark;

    @Autowired
    protected HdfsConfig hdfsConfig;

    @Autowired
    protected SparkConfig sparkConfig;

    @BeforeAll
    void setupSpark() {
        spark = sparkConfig.sparkSession();

        System.out.println("=== HDFS INTEGRATION TEST ===");
        System.out.println("Using HDFS URI: " + hdfsConfig.getUri());
        System.out.println("Using Parquet Path: " + hdfsConfig.getParquetPath());
        System.out.println("Full Parquet Path: " + hdfsConfig.getFullParquetPath());
    }

    @AfterAll
    static void tearDownSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    protected void logSeparator() {
        System.out.println("=".repeat(80));
    }

    protected void logHeader(String title) {
        logSeparator();
        System.out.println(title);
        logSeparator();
    }
}