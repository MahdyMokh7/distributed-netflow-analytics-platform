package com.nimbo.batch.job;

import com.nimbo.batch.config.BatchConfig;
import com.nimbo.batch.config.OutputConfig;
import com.nimbo.batch.discovery.FileDiscoveryService;
import com.nimbo.batch.processor.*;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class BatchJobRunnerTest {

    @Mock
    private TrafficAggregationProcessor trafficProcessor;

    @Mock
    private ConnectionProcessor connectionProcessor;

    @Mock
    private ProtocolProcessor protocolProcessor;

    @Mock
    private TopIpProcessor topIpProcessor;

    @Mock
    private BatchConfig batchConfig;

    @Mock
    private OutputConfig outputConfig;

    @Mock
    private FileDiscoveryService fileDiscoveryService;

    private BatchJobRunner batchJobRunner;

    @BeforeEach
    void setUp() {
        batchJobRunner = new BatchJobRunner(
                List.of(trafficProcessor, connectionProcessor, protocolProcessor, topIpProcessor),
                batchConfig,
                outputConfig,
                fileDiscoveryService
        );
    }

    @Test
    void shouldRunAllProcessorsWhenBatchEnabled() {
        when(batchConfig.isEnabled()).thenReturn(true);
        when(batchConfig.getWindowDuration()).thenReturn("1 hour");

        List<Path> mockFiles = List.of(
                new Path("hdfs://test/path/file1.parquet"),
                new Path("hdfs://test/path/file2.parquet")
        );
        when(fileDiscoveryService.discoverNewFiles(any(UUID.class))).thenReturn(mockFiles);

        batchJobRunner.runBatchJob();

        verify(fileDiscoveryService).discoverNewFiles(any(UUID.class));
        verify(trafficProcessor).process(anyList(), any(UUID.class));
        verify(connectionProcessor).process(anyList(), any(UUID.class));
        verify(protocolProcessor).process(anyList(), any(UUID.class));
        verify(topIpProcessor).process(anyList(), any(UUID.class));
        verify(fileDiscoveryService).markFilesAsProcessed(anyList(), any(UUID.class));
    }

    @Test
    void shouldSkipBatchJobWhenDisabled() {
        when(batchConfig.isEnabled()).thenReturn(false);

        batchJobRunner.runBatchJob();

        verify(fileDiscoveryService, never()).discoverNewFiles(any(UUID.class));
        verify(trafficProcessor, never()).process(anyList(), any(UUID.class));
        verify(connectionProcessor, never()).process(anyList(), any(UUID.class));
        verify(protocolProcessor, never()).process(anyList(), any(UUID.class));
        verify(topIpProcessor, never()).process(anyList(), any(UUID.class));
        verify(fileDiscoveryService, never()).markFilesAsProcessed(anyList(), any(UUID.class));
    }

    @Test
    void shouldSkipProcessingWhenNoFilesFound() {
        when(batchConfig.isEnabled()).thenReturn(true);
        when(batchConfig.getWindowDuration()).thenReturn("1 hour");
        when(fileDiscoveryService.discoverNewFiles(any(UUID.class))).thenReturn(List.of());

        batchJobRunner.runBatchJob();

        verify(fileDiscoveryService).discoverNewFiles(any(UUID.class));
        verify(trafficProcessor, never()).process(anyList(), any(UUID.class));
        verify(connectionProcessor, never()).process(anyList(), any(UUID.class));
        verify(protocolProcessor, never()).process(anyList(), any(UUID.class));
        verify(topIpProcessor, never()).process(anyList(), any(UUID.class));
        verify(fileDiscoveryService, never()).markFilesAsProcessed(anyList(), any(UUID.class));
    }

    @Test
    void shouldNotMarkFilesAsProcessedWhenProcessorFails() {
        when(batchConfig.isEnabled()).thenReturn(true);
        when(batchConfig.getWindowDuration()).thenReturn("1 hour");

        List<Path> mockFiles = List.of(
                new Path("hdfs://test/path/file1.parquet"),
                new Path("hdfs://test/path/file2.parquet")
        );
        when(fileDiscoveryService.discoverNewFiles(any(UUID.class))).thenReturn(mockFiles);

        doThrow(new RuntimeException("Processor failed"))
                .when(trafficProcessor).process(anyList(), any(UUID.class));

        assertThrows(RuntimeException.class, () -> batchJobRunner.runBatchJob());

        verify(fileDiscoveryService).discoverNewFiles(any(UUID.class));
        verify(trafficProcessor).process(anyList(), any(UUID.class));
        verify(connectionProcessor, never()).process(anyList(), any(UUID.class));
        verify(protocolProcessor, never()).process(anyList(), any(UUID.class));
        verify(topIpProcessor, never()).process(anyList(), any(UUID.class));
        verify(fileDiscoveryService, never()).markFilesAsProcessed(anyList(), any(UUID.class));
    }

    @Test
    void shouldHandleProcessorExceptionGracefully() {
        when(batchConfig.isEnabled()).thenReturn(true);
        when(batchConfig.getWindowDuration()).thenReturn("1 hour");

        List<Path> mockFiles = List.of(
                new Path("hdfs://test/path/file1.parquet"),
                new Path("hdfs://test/path/file2.parquet")
        );
        when(fileDiscoveryService.discoverNewFiles(any(UUID.class))).thenReturn(mockFiles);

        doThrow(new RuntimeException("Processor failed"))
                .when(trafficProcessor).process(anyList(), any(UUID.class));

        assertThrows(RuntimeException.class, () -> batchJobRunner.runBatchJob());

        verify(fileDiscoveryService).discoverNewFiles(any(UUID.class));
        verify(trafficProcessor).process(anyList(), any(UUID.class));
        verify(fileDiscoveryService, never()).markFilesAsProcessed(anyList(), any(UUID.class));
    }

    @Test
    void shouldProcessAllProcessorsInListOrder() {
        when(batchConfig.isEnabled()).thenReturn(true);
        when(batchConfig.getWindowDuration()).thenReturn("1 hour");

        List<Path> mockFiles = List.of(
                new Path("hdfs://test/path/file1.parquet"),
                new Path("hdfs://test/path/file2.parquet")
        );
        when(fileDiscoveryService.discoverNewFiles(any(UUID.class))).thenReturn(mockFiles);

        batchJobRunner.runBatchJob();

        var inOrder = inOrder(trafficProcessor, connectionProcessor, protocolProcessor, topIpProcessor);
        inOrder.verify(trafficProcessor).process(anyList(), any(UUID.class));
        inOrder.verify(connectionProcessor).process(anyList(), any(UUID.class));
        inOrder.verify(protocolProcessor).process(anyList(), any(UUID.class));
        inOrder.verify(topIpProcessor).process(anyList(), any(UUID.class));
    }

    @Test
    void shouldMarkFilesAsProcessedAfterAllProcessorsSucceed() {
        when(batchConfig.isEnabled()).thenReturn(true);
        when(batchConfig.getWindowDuration()).thenReturn("1 hour");

        List<Path> mockFiles = List.of(
                new Path("hdfs://test/path/file1.parquet"),
                new Path("hdfs://test/path/file2.parquet")
        );
        when(fileDiscoveryService.discoverNewFiles(any(UUID.class))).thenReturn(mockFiles);

        batchJobRunner.runBatchJob();

        verify(fileDiscoveryService).markFilesAsProcessed(eq(mockFiles), any(UUID.class));
    }

    @Test
    void shouldNotMarkFilesAsProcessedWhenFileDiscoveryFails() {
        when(batchConfig.isEnabled()).thenReturn(true);
        when(batchConfig.getWindowDuration()).thenReturn("1 hour");
        when(fileDiscoveryService.discoverNewFiles(any(UUID.class)))
                .thenThrow(new RuntimeException("Discovery failed"));

        assertThrows(RuntimeException.class, () -> batchJobRunner.runBatchJob());

        verify(fileDiscoveryService).discoverNewFiles(any(UUID.class));
        verify(fileDiscoveryService, never()).markFilesAsProcessed(anyList(), any(UUID.class));
        verify(trafficProcessor, never()).process(anyList(), any(UUID.class));
    }
}