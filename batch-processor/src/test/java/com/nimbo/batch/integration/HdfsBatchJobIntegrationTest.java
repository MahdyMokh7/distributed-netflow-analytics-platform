package com.nimbo.batch.integration;

import com.nimbo.batch.processor.ConnectionProcessor;
import com.nimbo.batch.processor.ProtocolProcessor;
import com.nimbo.batch.processor.TopIpProcessor;
import com.nimbo.batch.processor.TrafficAggregationProcessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;

public class HdfsBatchJobIntegrationTest extends HdfsIntegrationTestBase {

    private TrafficAggregationProcessor trafficProcessor;
    private ConnectionProcessor connectionProcessor;
    private ProtocolProcessor protocolProcessor;
    private TopIpProcessor topIpProcessor;

    @BeforeEach
    void setUp() {
        trafficProcessor = new TrafficAggregationProcessor(spark, hdfsConfig);
        connectionProcessor = new ConnectionProcessor(spark, hdfsConfig);
        protocolProcessor = new ProtocolProcessor(spark, hdfsConfig);
        topIpProcessor = new TopIpProcessor(spark, hdfsConfig);

        logHeader("=== TEST: Full Batch Job Against HDFS ===");
        System.out.println("Reading from: " + hdfsConfig.getFullParquetPath());
    }

    @Test
    void shouldRunAllProcessorsAgainstHdfsWithoutException() {
        assertThatCode(() -> {
            trafficProcessor.process();
            connectionProcessor.process();
            protocolProcessor.process();
            topIpProcessor.process();
        }).doesNotThrowAnyException();
    }
}