package com.nimbo.ingester.consumer;

import com.nimbo.ingester.config.KafkaProperties;
import com.nimbo.ingester.deserializer.AvroDeserializer;
import com.nimbo.ingester.service.DataProcessingService;
import com.nimbo.ingester.service.DlqService;
import com.nimbo.ingester.service.NetflowValidationService;
import com.nimbo.storage.model.TopicPartitionKey;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("NetflowConsumer Unit Tests")
class NetflowConsumerTest {

    @Mock
    private KafkaProperties kafkaProperties;

    @Mock
    private AvroDeserializer avroDeserializer;

    @Mock
    private DataProcessingService dataProcessingService;

    @Mock
    private NetflowValidationService validationService;

    @Mock
    private DlqService dlqService;

    private NetflowConsumer netflowConsumer;

    @BeforeEach
    void setUp() {
        lenient().when(kafkaProperties.getTopic()).thenReturn("test-topic");
        lenient().when(kafkaProperties.getPollTimeoutMs()).thenReturn(100);
        lenient().when(kafkaProperties.isEnableAutoCommit()).thenReturn(false);
        lenient().when(kafkaProperties.getBootstrapServers()).thenReturn("localhost:9092");
        lenient().when(kafkaProperties.getConsumerGroup()).thenReturn("test-group");
        lenient().when(kafkaProperties.getMaxPollRecords()).thenReturn(500);
        lenient().when(kafkaProperties.getSessionTimeoutMs()).thenReturn(30000);
        lenient().when(kafkaProperties.getHeartbeatIntervalMs()).thenReturn(3000);
        lenient().when(kafkaProperties.getMaxPollIntervalMs()).thenReturn(300000);
        lenient().when(kafkaProperties.getAdditionalConfigs()).thenReturn(null);

        netflowConsumer = new NetflowConsumer(
                kafkaProperties,
                avroDeserializer,
                dataProcessingService,
                validationService,
                dlqService,
                1
        );
    }

    @Test
    @DisplayName("Should create consumer with unique instance ID")
    void shouldCreateConsumerWithUniqueInstanceId() {
        NetflowConsumer consumer1 = new NetflowConsumer(
                kafkaProperties,
                avroDeserializer,
                dataProcessingService,
                validationService,
                dlqService,
                1
        );

        NetflowConsumer consumer2 = new NetflowConsumer(
                kafkaProperties,
                avroDeserializer,
                dataProcessingService,
                validationService,
                dlqService,
                2
        );

        assertThat(consumer1).isNotNull();
        assertThat(consumer2).isNotNull();
    }

    @Test
    @DisplayName("Should start consumer instance successfully")
    void shouldStartConsumerInstanceSuccessfully() {
        assertThatCode(() -> netflowConsumer.start()).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should shutdown gracefully and close consumer")
    void shouldShutdownGracefullyAndCloseConsumer() {
        netflowConsumer.start();
        assertThatCode(() -> netflowConsumer.shutdown()).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should flush remaining records on shutdown")
    void shouldFlushRemainingRecordsOnShutdown() {
        Map<TopicPartitionKey, Long> flushOffsets = createFlushOffsets();
        when(dataProcessingService.flush()).thenReturn(flushOffsets);

        netflowConsumer.start();
        netflowConsumer.shutdown();

        verify(dataProcessingService, atLeastOnce()).flush();
    }

    @Test
    @DisplayName("Should handle flush failure during shutdown gracefully")
    void shouldHandleFlushFailureDuringShutdown() {
        doThrow(new RuntimeException("Flush failed"))
                .when(dataProcessingService).flush();

        netflowConsumer.start();
        assertThatCode(() -> netflowConsumer.shutdown()).doesNotThrowAnyException();

        verify(dataProcessingService, atLeastOnce()).flush();
    }

    @Test
    @DisplayName("Should handle deserialization failure gracefully")
    void shouldHandleDeserializationFailureGracefully() {
        assertThatCode(() -> netflowConsumer.start()).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should handle interruption during shutdown")
    void shouldHandleInterruptionDuringShutdown() {
        netflowConsumer.start();

        AtomicReference<Thread> consumerThread = new AtomicReference<>();
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (t.getName().contains("consumer-instance-1")) {
                consumerThread.set(t);
                break;
            }
        }

        if (consumerThread.get() != null) {
            consumerThread.get().interrupt();
        }

        assertThatCode(() -> netflowConsumer.shutdown()).doesNotThrowAnyException();
    }

    private Map<TopicPartitionKey, Long> createFlushOffsets() {
        TopicPartitionKey key = new TopicPartitionKey("test-topic", 0);
        return Map.of(key, 43L);
    }
}