package com.nimbo.batch.job;

import com.nimbo.batch.config.BatchConfig;
import com.nimbo.batch.config.OutputConfig;
import com.nimbo.batch.processor.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.junit.jupiter.api.Assertions;

import java.util.List;

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
        when(outputConfig.getMode()).thenReturn("console");

        batchJobRunner.runBatchJob();

        verify(trafficProcessor).process();
        verify(connectionProcessor).process();
        verify(protocolProcessor).process();
        verify(topIpProcessor).process();
    }

    @Test
    void shouldSkipBatchJobWhenDisabled() {
        when(batchConfig.isEnabled()).thenReturn(false);

        batchJobRunner.runBatchJob();

        verify(trafficProcessor, never()).process();
        verify(connectionProcessor, never()).process();
        verify(protocolProcessor, never()).process();
        verify(topIpProcessor, never()).process();
    }

    @Test
    void shouldHandleProcessorExceptionGracefully() {
        when(batchConfig.isEnabled()).thenReturn(true);
        when(outputConfig.getMode()).thenReturn("console");
        doThrow(new RuntimeException("Processor failed"))
                .when(trafficProcessor).process();

        Assertions.assertThrows(
                RuntimeException.class,
                () -> batchJobRunner.runBatchJob()
        );
    }

    @Test
    void shouldLogOutputModeBeforeExecution() {
        when(batchConfig.isEnabled()).thenReturn(true);
        when(outputConfig.getMode()).thenReturn("console");

        batchJobRunner.runBatchJob();

        verify(trafficProcessor).process();
        verify(connectionProcessor).process();
        verify(protocolProcessor).process();
        verify(topIpProcessor).process();
    }
}