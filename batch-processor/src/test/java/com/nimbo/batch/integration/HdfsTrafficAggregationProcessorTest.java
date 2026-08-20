package com.nimbo.batch.integration;

import com.nimbo.batch.config.HdfsConfig;
import com.nimbo.batch.processor.TrafficAggregationProcessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.assertj.core.api.Assertions.assertThatCode;

public class HdfsTrafficAggregationProcessorTest extends HdfsIntegrationTestBase {

    private TrafficAggregationProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new TrafficAggregationProcessor(spark, hdfsConfig);
        logHeader("=== TEST: Traffic Aggregation from HDFS ===");
        System.out.println("Reading from: " + hdfsConfig.getFullParquetPath());
    }

    @Test
    void shouldProcessTrafficAggregationFromHdfs() {
        assertThatCode(() -> processor.process())
                .doesNotThrowAnyException();
    }
}