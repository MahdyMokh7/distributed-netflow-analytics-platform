package com.nimbo.batch.integration;

import com.nimbo.batch.config.HdfsConfig;
import com.nimbo.batch.processor.TopIpProcessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.assertj.core.api.Assertions.assertThatCode;

public class HdfsTopIpProcessorTest extends HdfsIntegrationTestBase {

    private TopIpProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new TopIpProcessor(spark, hdfsConfig);
        logHeader("=== TEST: Top IP Aggregation from HDFS ===");
        System.out.println("Reading from: " + hdfsConfig.getFullParquetPath());
    }

    @Test
    void shouldProcessTopIpAggregationFromHdfs() {
        assertThatCode(() -> processor.process())
                .doesNotThrowAnyException();
    }
}