package com.nimbo.batch.processor;

import com.nimbo.batch.config.BatchConfig;
import com.nimbo.batch.config.HdfsConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TopIpProcessorTest extends BaseProcessorTest {

    private HdfsConfig hdfsConfig;
    private BatchConfig batchConfig;
    private TopIpProcessor processor;

    @BeforeEach
    void setUp() {
        hdfsConfig = mock(HdfsConfig.class);
        when(hdfsConfig.getFullParquetPath())
                .thenReturn(testParquetConfig.getLocalPath());
        batchConfig = mock(BatchConfig.class);
        when(batchConfig.getWindowDuration()).thenReturn("1 hour");
        processor = new TopIpProcessor(spark, hdfsConfig, batchConfig);
    }

    @Test
    void shouldProcessWithConsoleOutputWithoutException() {
        processor.process();
    }

    @Test
    void shouldThrowExceptionOnEmptyDataset() {
        when(hdfsConfig.getFullParquetPath())
                .thenReturn(testParquetConfig.getEmptyPath());
        TopIpProcessor emptyProcessor = new TopIpProcessor(spark, hdfsConfig, batchConfig);

        assertThatThrownBy(emptyProcessor::process)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Top IP aggregation failed");
    }

    @Test
    void shouldThrowExceptionOnInvalidPath() {
        when(hdfsConfig.getFullParquetPath())
                .thenReturn(testParquetConfig.getInvalidPath());
        TopIpProcessor invalidProcessor = new TopIpProcessor(spark, hdfsConfig, batchConfig);

        assertThatThrownBy(invalidProcessor::process)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Top IP aggregation failed");
    }
}