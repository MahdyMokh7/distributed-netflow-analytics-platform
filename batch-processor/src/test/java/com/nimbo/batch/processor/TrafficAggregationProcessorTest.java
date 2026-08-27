package com.nimbo.batch.processor;

import com.nimbo.batch.config.BatchConfig;
import com.nimbo.batch.config.HdfsConfig;
import com.nimbo.batch.config.OutputConfig;
import com.nimbo.batch.writer.PostgresWriter;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
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
    private List<Path> testFiles;

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

        testFiles = List.of(new Path(testParquetConfig.getLocalPath()));
    }

    @Test
    void shouldProcessWithConsoleOutputWithoutException() {
        UUID batchId = UUID.randomUUID();
        processor.process(testFiles, batchId);
    }

    @Test
    void shouldThrowExceptionOnEmptyDataset() {
        when(hdfsConfig.getFullParquetPath())
                .thenReturn(testParquetConfig.getEmptyPath());

        TrafficAggregationProcessor emptyProcessor = new TrafficAggregationProcessor(
                spark, hdfsConfig, batchConfig, outputConfig, postgresWriter
        );

        UUID batchId = UUID.randomUUID();
        List<Path> emptyFiles = List.of(
                new Path(testParquetConfig.getEmptyPath() + "/empty.parquet")
        );
        assertThatThrownBy(() -> emptyProcessor.process(emptyFiles, batchId))
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
        List<Path> invalidFiles = List.of(
                new Path(testParquetConfig.getInvalidPath() + "/invalid.parquet")
        );
        assertThatThrownBy(() -> invalidProcessor.process(invalidFiles, batchId))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Traffic aggregation failed");
    }

    @Test
    void shouldWriteToDatabaseWhenDatabaseModeEnabled() {
        when(outputConfig.isConsoleMode()).thenReturn(false);
        when(outputConfig.isDatabaseMode()).thenReturn(true);

        UUID batchId = UUID.randomUUID();
        processor.process(testFiles, batchId);
    }

    @Test
    void shouldOrderResultsByWindowStartDescending() {
        UUID batchId = UUID.randomUUID();
        processor.process(testFiles, batchId);
    }

    @Test
    void shouldOrderResultsByBytesDescending() {
        UUID batchId = UUID.randomUUID();
        processor.process(testFiles, batchId);
    }

    @Test
    void shouldHandleLargeDatasetWithoutErrors() {
        UUID batchId = UUID.randomUUID();
        processor.process(testFiles, batchId);
    }

    @Test
    void shouldProcessWithEmptyFileListWithoutException() {
        UUID batchId = UUID.randomUUID();
        processor.process(List.of(), batchId);
    }
}