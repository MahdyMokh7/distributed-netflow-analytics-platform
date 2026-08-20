package com.nimbo.batch.processor;

import com.nimbo.batch.config.HdfsConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ProtocolProcessorTest extends BaseProcessorTest {

    private HdfsConfig hdfsConfig;
    private ProtocolProcessor processor;

    @BeforeEach
    void setUp() {
        hdfsConfig = mock(HdfsConfig.class);
        when(hdfsConfig.getFullParquetPath())
                .thenReturn(testParquetConfig.getLocalPath());
        processor = new ProtocolProcessor(spark, hdfsConfig);
    }

    @Test
    void shouldProcessWithConsoleOutputWithoutException() {
        processor.process();
    }

    @Test
    void shouldThrowExceptionOnEmptyDataset() {
        when(hdfsConfig.getFullParquetPath())
                .thenReturn(testParquetConfig.getEmptyPath());
        ProtocolProcessor emptyProcessor = new ProtocolProcessor(spark, hdfsConfig);

        assertThatThrownBy(emptyProcessor::process)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Protocol aggregation failed");
    }

    @Test
    void shouldThrowExceptionOnInvalidPath() {
        when(hdfsConfig.getFullParquetPath())
                .thenReturn(testParquetConfig.getInvalidPath());
        ProtocolProcessor invalidProcessor = new ProtocolProcessor(spark, hdfsConfig);

        assertThatThrownBy(invalidProcessor::process)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Protocol aggregation failed");
    }
}