package com.nimbo.batch.processor;

import com.nimbo.batch.config.HdfsConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TrafficAggregationProcessorTest extends BaseProcessorTest {

    private HdfsConfig hdfsConfig;
    private TrafficAggregationProcessor processor;

    @BeforeEach
    void setUp() {
        hdfsConfig = mock(HdfsConfig.class);
        when(hdfsConfig.getFullParquetPath())
                .thenReturn(testParquetConfig.getLocalPath());
        processor = new TrafficAggregationProcessor(spark, hdfsConfig);
    }

    @Test
    void shouldProcessWithConsoleOutputWithoutException() {
        processor.process();
    }

    @Test
    void shouldThrowExceptionOnEmptyDataset() {
        when(hdfsConfig.getFullParquetPath())
                .thenReturn(testParquetConfig.getEmptyPath());
        TrafficAggregationProcessor emptyProcessor = new TrafficAggregationProcessor(spark, hdfsConfig);

        assertThatThrownBy(emptyProcessor::process)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Traffic aggregation failed");
    }

    @Test
    void shouldThrowExceptionOnInvalidPath() {
        when(hdfsConfig.getFullParquetPath())
                .thenReturn(testParquetConfig.getInvalidPath());
        TrafficAggregationProcessor invalidProcessor = new TrafficAggregationProcessor(spark, hdfsConfig);

        assertThatThrownBy(invalidProcessor::process)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Traffic aggregation failed");
    }
}