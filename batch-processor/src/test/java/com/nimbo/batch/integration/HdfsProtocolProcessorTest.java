package com.nimbo.batch.integration;

import com.nimbo.batch.config.HdfsConfig;
import com.nimbo.batch.processor.ProtocolProcessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.assertj.core.api.Assertions.assertThatCode;

public class HdfsProtocolProcessorTest extends HdfsIntegrationTestBase {

    private ProtocolProcessor processor;

    @BeforeEach
    void setUp() {
        processor = new ProtocolProcessor(spark, hdfsConfig);
        logHeader("=== TEST: Protocol Aggregation from HDFS ===");
        System.out.println("Reading from: " + hdfsConfig.getFullParquetPath());
    }

    @Test
    void shouldProcessProtocolAggregationFromHdfs() {
        assertThatCode(() -> processor.process())
                .doesNotThrowAnyException();
    }
}