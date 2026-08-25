package com.nimbo.batch.job;

import com.nimbo.batch.config.BatchConfig;
import com.nimbo.batch.config.OutputConfig;
import com.nimbo.batch.processor.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;
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

    private BatchJobRunner batchJobRunner;

    @BeforeEach
    void setUp() {
        batchJobRunner = new BatchJobRunner(
                List.of(trafficProcessor, connectionProcessor, protocolProcessor, topIpProcessor),
                batchConfig,
                outputConfig
        );
    }

    @Test
    void shouldRunAllProcessorsWhenBatchEnabled() {
        when(batchConfig.isEnabled()).thenReturn(true);
        when(batchConfig.getWindowDuration()).thenReturn("1 hour");

        batchJobRunner.runBatchJob();

        verify(trafficProcessor).process(any(UUID.class));
        verify(connectionProcessor).process(any(UUID.class));
        verify(protocolProcessor).process(any(UUID.class));
        verify(topIpProcessor).process(any(UUID.class));
    }

    @Test
    void shouldSkipBatchJobWhenDisabled() {
        when(batchConfig.isEnabled()).thenReturn(false);

        batchJobRunner.runBatchJob();

        verify(trafficProcessor, never()).process(any(UUID.class));
        verify(connectionProcessor, never()).process(any(UUID.class));
        verify(protocolProcessor, never()).process(any(UUID.class));
        verify(topIpProcessor, never()).process(any(UUID.class));
    }

    @Test
    void shouldHandleProcessorExceptionGracefully() {
        when(batchConfig.isEnabled()).thenReturn(true);
        when(batchConfig.getWindowDuration()).thenReturn("1 hour");

        doThrow(new RuntimeException("Processor failed"))
                .when(trafficProcessor).process(any(UUID.class));

        assertThrows(
                RuntimeException.class,
                () -> batchJobRunner.runBatchJob()
        );
    }

    @Test
    void shouldLogOutputModeBeforeExecution() {
        when(batchConfig.isEnabled()).thenReturn(true);
        when(batchConfig.getWindowDuration()).thenReturn("1 hour");

        batchJobRunner.runBatchJob();

        verify(trafficProcessor).process(any(UUID.class));
        verify(connectionProcessor).process(any(UUID.class));
        verify(protocolProcessor).process(any(UUID.class));
        verify(topIpProcessor).process(any(UUID.class));
    }

    @Test
    void shouldGenerateBatchIdAndPassToAllProcessors() {
        when(batchConfig.isEnabled()).thenReturn(true);
        when(batchConfig.getWindowDuration()).thenReturn("1 hour");

        batchJobRunner.runBatchJob();

        verify(trafficProcessor).process(any(UUID.class));
        verify(connectionProcessor).process(any(UUID.class));
        verify(protocolProcessor).process(any(UUID.class));
        verify(topIpProcessor).process(any(UUID.class));
    }

    @Test
    void shouldHandleSingleProcessorFailureWithoutAffectingOthers() {
        when(batchConfig.isEnabled()).thenReturn(true);
        when(batchConfig.getWindowDuration()).thenReturn("1 hour");

        doThrow(new RuntimeException("Connection processor failed"))
                .when(connectionProcessor).process(any(UUID.class));

        assertThrows(RuntimeException.class, () -> batchJobRunner.runBatchJob());

        verify(trafficProcessor).process(any(UUID.class));
        verify(connectionProcessor).process(any(UUID.class));
        verify(topIpProcessor, never()).process(any(UUID.class));
        verify(protocolProcessor, never()).process(any(UUID.class));
    }

    @Test
    void shouldWriteToDatabaseWhenDatabaseModeEnabled() {
        when(batchConfig.isEnabled()).thenReturn(true);
        when(batchConfig.getWindowDuration()).thenReturn("1 hour");

        batchJobRunner.runBatchJob();

        verify(trafficProcessor).process(any(UUID.class));
        verify(connectionProcessor).process(any(UUID.class));
        verify(protocolProcessor).process(any(UUID.class));
        verify(topIpProcessor).process(any(UUID.class));
    }

    @Test
    void shouldWriteToBothConsoleAndDatabaseWhenBothModeEnabled() {
        when(batchConfig.isEnabled()).thenReturn(true);
        when(batchConfig.getWindowDuration()).thenReturn("1 hour");

        batchJobRunner.runBatchJob();

        verify(trafficProcessor).process(any(UUID.class));
        verify(connectionProcessor).process(any(UUID.class));
        verify(protocolProcessor).process(any(UUID.class));
        verify(topIpProcessor).process(any(UUID.class));
    }

    @Test
    void shouldLogBatchIdWhenJobStarts() {
        when(batchConfig.isEnabled()).thenReturn(true);
        when(batchConfig.getWindowDuration()).thenReturn("1 hour");

        batchJobRunner.runBatchJob();

        verify(trafficProcessor).process(any(UUID.class));
        verify(connectionProcessor).process(any(UUID.class));
        verify(protocolProcessor).process(any(UUID.class));
        verify(topIpProcessor).process(any(UUID.class));
    }

    @Test
    void shouldLogProcessorStartAndCompletion() {
        when(batchConfig.isEnabled()).thenReturn(true);
        when(batchConfig.getWindowDuration()).thenReturn("1 hour");

        batchJobRunner.runBatchJob();

        verify(trafficProcessor, times(1)).process(any(UUID.class));
        verify(connectionProcessor, times(1)).process(any(UUID.class));
        verify(protocolProcessor, times(1)).process(any(UUID.class));
        verify(topIpProcessor, times(1)).process(any(UUID.class));
    }

    @Test
    void shouldLogJobCompletionWithDuration() {
        when(batchConfig.isEnabled()).thenReturn(true);
        when(batchConfig.getWindowDuration()).thenReturn("1 hour");

        batchJobRunner.runBatchJob();

        verify(trafficProcessor).process(any(UUID.class));
        verify(connectionProcessor).process(any(UUID.class));
        verify(protocolProcessor).process(any(UUID.class));
        verify(topIpProcessor).process(any(UUID.class));
    }

    @Test
    void shouldLogErrorWhenProcessorFails() {
        when(batchConfig.isEnabled()).thenReturn(true);
        when(batchConfig.getWindowDuration()).thenReturn("1 hour");

        doThrow(new RuntimeException("Processor failed"))
                .when(trafficProcessor).process(any(UUID.class));

        assertThrows(RuntimeException.class, () -> batchJobRunner.runBatchJob());

        verify(trafficProcessor).process(any(UUID.class));
        verify(connectionProcessor, never()).process(any(UUID.class));
        verify(protocolProcessor, never()).process(any(UUID.class));
        verify(topIpProcessor, never()).process(any(UUID.class));
    }

    @Test
    void shouldLogBatchIdOnJobCompletion() {
        when(batchConfig.isEnabled()).thenReturn(true);
        when(batchConfig.getWindowDuration()).thenReturn("1 hour");

        batchJobRunner.runBatchJob();

        verify(trafficProcessor).process(any(UUID.class));
        verify(connectionProcessor).process(any(UUID.class));
        verify(protocolProcessor).process(any(UUID.class));
        verify(topIpProcessor).process(any(UUID.class));
    }

    @Test
    void shouldLogBatchIdOnJobFailure() {
        when(batchConfig.isEnabled()).thenReturn(true);
        when(batchConfig.getWindowDuration()).thenReturn("1 hour");

        doThrow(new RuntimeException("Processor failed"))
                .when(trafficProcessor).process(any(UUID.class));

        assertThrows(RuntimeException.class, () -> batchJobRunner.runBatchJob());

        verify(trafficProcessor).process(any(UUID.class));
        verify(connectionProcessor, never()).process(any(UUID.class));
    }

    @Test
    void shouldHandleMultipleProcessorFailures() {
        when(batchConfig.isEnabled()).thenReturn(true);
        when(batchConfig.getWindowDuration()).thenReturn("1 hour");

        doThrow(new RuntimeException("Connection failed"))
                .when(connectionProcessor).process(any(UUID.class));

        assertThrows(RuntimeException.class, () -> batchJobRunner.runBatchJob());

        verify(trafficProcessor).process(any(UUID.class));
        verify(connectionProcessor).process(any(UUID.class));
        verify(protocolProcessor, never()).process(any(UUID.class));
        verify(topIpProcessor, never()).process(any(UUID.class));
    }

    @Test
    void shouldProcessAllProcessorsInListOrder() {
        when(batchConfig.isEnabled()).thenReturn(true);
        when(batchConfig.getWindowDuration()).thenReturn("1 hour");

        batchJobRunner.runBatchJob();

        var inOrder = inOrder(trafficProcessor, connectionProcessor, protocolProcessor, topIpProcessor);
        inOrder.verify(trafficProcessor).process(any(UUID.class));
        inOrder.verify(connectionProcessor).process(any(UUID.class));
        inOrder.verify(protocolProcessor).process(any(UUID.class));
        inOrder.verify(topIpProcessor).process(any(UUID.class));
    }
}