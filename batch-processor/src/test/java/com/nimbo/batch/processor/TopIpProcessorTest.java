package com.nimbo.batch.processor;

import com.nimbo.batch.config.HdfsConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TopIpProcessorTest extends BaseProcessorTest {

    private HdfsConfig hdfsConfig;
    private TopIpProcessor processor;

    @BeforeEach
    void setUp() {
        hdfsConfig = mock(HdfsConfig.class);
        when(hdfsConfig.getFullParquetPath())
                .thenReturn(testParquetConfig.getLocalPath());
        processor = new TopIpProcessor(spark, hdfsConfig);
    }

    @Test
    void shouldProcessWithConsoleOutputWithoutException() {
        processor.process();
    }

    @Test
    void shouldThrowExceptionOnEmptyDataset() {
        when(hdfsConfig.getFullParquetPath())
                .thenReturn(testParquetConfig.getEmptyPath());
        TopIpProcessor emptyProcessor = new TopIpProcessor(spark, hdfsConfig);

        assertThatThrownBy(emptyProcessor::process)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Top IP aggregation failed");
    }

    @Test
    void shouldThrowExceptionOnInvalidPath() {
        when(hdfsConfig.getFullParquetPath())
                .thenReturn(testParquetConfig.getInvalidPath());
        TopIpProcessor invalidProcessor = new TopIpProcessor(spark, hdfsConfig);

        assertThatThrownBy(invalidProcessor::process)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Top IP aggregation failed");
    }
}