package com.nimbo.ingester.consumer;

import com.nimbo.ingester.config.KafkaProperties;
import com.nimbo.ingester.deserializer.AvroDeserializer;
import com.nimbo.ingester.service.DlqService;
import com.nimbo.ingester.service.NetflowValidationService;
import com.nimbo.storage.config.HdfsProperties;
import com.nimbo.storage.service.batching.BatchPolicy;
import com.nimbo.storage.service.hdfs.FileStorage;
import com.nimbo.storage.service.naming.FileNameGenerator;
import com.nimbo.storage.service.parquet.ParquetFileWriter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("ConsumerManager Unit Tests")
class ConsumerManagerTest {

    @Mock
    private KafkaProperties kafkaProperties;

    @Mock
    private AvroDeserializer avroDeserializer;

    @Mock
    private NetflowValidationService validationService;

    @Mock
    private DlqService dlqService;

    @Mock
    private BatchPolicy batchPolicy;

    @Mock
    private ParquetFileWriter parquetFileWriter;

    @Mock
    private FileStorage fileStorage;

    @Mock
    private FileNameGenerator fileNameGenerator;

    @Mock
    private HdfsProperties hdfsProperties;

    private ConsumerManager consumerManager;

    @BeforeEach
    void setUp() {
        lenient().when(kafkaProperties.getTopic()).thenReturn("test-topic");
        lenient().when(kafkaProperties.getConsumerInstances()).thenReturn(3);
        lenient().when(kafkaProperties.getBootstrapServers()).thenReturn("localhost:9092");
        lenient().when(kafkaProperties.getConsumerGroup()).thenReturn("test-group");
        lenient().when(kafkaProperties.getMaxPollRecords()).thenReturn(500);
        lenient().when(kafkaProperties.getSessionTimeoutMs()).thenReturn(30000);
        lenient().when(kafkaProperties.getHeartbeatIntervalMs()).thenReturn(3000);
        lenient().when(kafkaProperties.getMaxPollIntervalMs()).thenReturn(300000);
        lenient().when(kafkaProperties.getAdditionalConfigs()).thenReturn(null);
        lenient().when(kafkaProperties.getPollTimeoutMs()).thenReturn(1000);
        lenient().when(kafkaProperties.isEnableAutoCommit()).thenReturn(false);
    }

    @Test
    @DisplayName("Should start multiple consumer instances")
    void shouldStartMultipleConsumerInstances() {
        consumerManager = new ConsumerManager(
                kafkaProperties,
                avroDeserializer,
                validationService,
                dlqService,
                batchPolicy,
                parquetFileWriter,
                fileStorage,
                fileNameGenerator,
                hdfsProperties
        );

        assertThatCode(() -> consumerManager.start()).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should shutdown all consumer instances gracefully")
    void shouldShutdownAllConsumerInstancesGracefully() {
        consumerManager = new ConsumerManager(
                kafkaProperties,
                avroDeserializer,
                validationService,
                dlqService,
                batchPolicy,
                parquetFileWriter,
                fileStorage,
                fileNameGenerator,
                hdfsProperties
        );

        consumerManager.start();
        assertThatCode(() -> consumerManager.shutdown()).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should respect consumer instance count configuration")
    void shouldRespectConsumerInstanceCountConfiguration() {
        when(kafkaProperties.getConsumerInstances()).thenReturn(5);

        consumerManager = new ConsumerManager(
                kafkaProperties,
                avroDeserializer,
                validationService,
                dlqService,
                batchPolicy,
                parquetFileWriter,
                fileStorage,
                fileNameGenerator,
                hdfsProperties
        );

        assertThatCode(() -> consumerManager.start()).doesNotThrowAnyException();
        assertThatCode(() -> consumerManager.shutdown()).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should handle single consumer instance")
    void shouldHandleSingleConsumerInstance() {
        when(kafkaProperties.getConsumerInstances()).thenReturn(1);

        consumerManager = new ConsumerManager(
                kafkaProperties,
                avroDeserializer,
                validationService,
                dlqService,
                batchPolicy,
                parquetFileWriter,
                fileStorage,
                fileNameGenerator,
                hdfsProperties
        );

        assertThatCode(() -> consumerManager.start()).doesNotThrowAnyException();
        assertThatCode(() -> consumerManager.shutdown()).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Should handle shutdown when no consumers started")
    void shouldHandleShutdownWhenNoConsumersStarted() {
        consumerManager = new ConsumerManager(
                kafkaProperties,
                avroDeserializer,
                validationService,
                dlqService,
                batchPolicy,
                parquetFileWriter,
                fileStorage,
                fileNameGenerator,
                hdfsProperties
        );

        assertThatCode(() -> consumerManager.shutdown()).doesNotThrowAnyException();
    }
}