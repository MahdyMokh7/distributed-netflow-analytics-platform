package com.nimbo.batch.integration;

import com.nimbo.batch.config.HdfsConfig;
import com.nimbo.batch.processor.ConnectionProcessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.assertj.core.api.Assertions.assertThatCode;

public class HdfsConnectionProcessorTest extends HdfsIntegrationTestBase {

    private ConnectionProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new ConnectionProcessor(spark, hdfsConfig);
        logHeader("=== TEST: Connection Aggregation from HDFS ===");
        System.out.println("Reading from: " + hdfsConfig.getFullParquetPath());
    }

    @Test
    void shouldProcessConnectionAggregationFromHdfs() {
        assertThatCode(() -> processor.process())
                .doesNotThrowAnyException();
    }
}