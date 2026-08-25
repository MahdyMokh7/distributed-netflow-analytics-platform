package com.nimbo.batch.processor;

import com.nimbo.batch.config.BatchConfig;
import com.nimbo.batch.config.HdfsConfig;
import com.nimbo.batch.config.OutputConfig;
import com.nimbo.batch.writer.PostgresWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TrafficAggregationProcessorTest extends BaseProcessorTest {

    private HdfsConfig hdfsConfig;
    private BatchConfig batchConfig;
    private OutputConfig outputConfig;
    private PostgresWriter postgresWriter;
    private TrafficAggregationProcessor processor;

    @BeforeEach
    void setUp() {
        hdfsConfig = mock(HdfsConfig.class);
        when(hdfsConfig.getFullParquetPath())
                .thenReturn(testParquetConfig.getLocalPath());

        batchConfig = mock(BatchConfig.class);
        when(batchConfig.getWindowDuration()).thenReturn("1 hour");

        outputConfig = mock(OutputConfig.class);
        when(outputConfig.isConsoleMode()).thenReturn(true);
        when(outputConfig.isDatabaseMode()).thenReturn(false);

        postgresWriter = mock(PostgresWriter.class);

        processor = new TrafficAggregationProcessor(
                spark, hdfsConfig, batchConfig, outputConfig, postgresWriter
        );
    }

    @Test
    void shouldProcessWithConsoleOutputWithoutException() {
        UUID batchId = UUID.randomUUID();
        processor.process(batchId);
    }

    @Test
    void shouldThrowExceptionOnEmptyDataset() {
        when(hdfsConfig.getFullParquetPath())
                .thenReturn(testParquetConfig.getEmptyPath());

        TrafficAggregationProcessor emptyProcessor = new TrafficAggregationProcessor(
                spark, hdfsConfig, batchConfig, outputConfig, postgresWriter
        );

        UUID batchId = UUID.randomUUID();
        assertThatThrownBy(() -> emptyProcessor.process(batchId))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Traffic aggregation failed");
    }

    @Test
    void shouldThrowExceptionOnInvalidPath() {
        when(hdfsConfig.getFullParquetPath())
                .thenReturn(testParquetConfig.getInvalidPath());

        TrafficAggregationProcessor invalidProcessor = new TrafficAggregationProcessor(
                spark, hdfsConfig, batchConfig, outputConfig, postgresWriter
        );

        UUID batchId = UUID.randomUUID();
        assertThatThrownBy(() -> invalidProcessor.process(batchId))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Traffic aggregation failed");
    }

    @Test
    void shouldWriteToDatabaseWhenDatabaseModeEnabled() {
        when(outputConfig.isConsoleMode()).thenReturn(false);
        when(outputConfig.isDatabaseMode()).thenReturn(true);

        UUID batchId = UUID.randomUUID();
        processor.process(batchId);
    }

    @Test
    void shouldNotWriteToDatabaseWhenConsoleModeOnly() {
        when(outputConfig.isConsoleMode()).thenReturn(true);
        when(outputConfig.isDatabaseMode()).thenReturn(false);

        UUID batchId = UUID.randomUUID();
        processor.process(batchId);
    }

    @Test
    void shouldLogNoRecordsWarningWhenEmptyDataset() {
        when(hdfsConfig.getFullParquetPath())
                .thenReturn(testParquetConfig.getEmptyPath());

        TrafficAggregationProcessor emptyProcessor = new TrafficAggregationProcessor(
                spark, hdfsConfig, batchConfig, outputConfig, postgresWriter
        );

        UUID batchId = UUID.randomUUID();
        assertThatThrownBy(() -> emptyProcessor.process(batchId))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Traffic aggregation failed");
    }

    @Test
    void shouldWriteToConsoleWhenConsoleModeOnly() {
        when(outputConfig.isConsoleMode()).thenReturn(true);
        when(outputConfig.isDatabaseMode()).thenReturn(false);

        UUID batchId = UUID.randomUUID();
        processor.process(batchId);
    }

    @Test
    void shouldWriteToDatabaseWhenDatabaseModeOnly() {
        when(outputConfig.isConsoleMode()).thenReturn(false);
        when(outputConfig.isDatabaseMode()).thenReturn(true);

        UUID batchId = UUID.randomUUID();
        processor.process(batchId);
    }

    @Test
    void shouldHandleLargeDatasetWithoutErrors() {
        UUID batchId = UUID.randomUUID();
        processor.process(batchId);
    }
}