package com.nimbo.ingester.consumer;

import com.nimbo.ingester.config.KafkaProperties;
import com.nimbo.ingester.deserializer.AvroDeserializer;
import com.nimbo.ingester.model.IngestedRecord;
import com.nimbo.ingester.service.DataProcessingService;
import com.nimbo.storage.model.TopicPartitionKey;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("NetflowConsumer Unit Tests")
class NetflowConsumerTest {

    @Mock
    private KafkaConsumer<byte[], byte[]> kafkaConsumer;

    @Mock
    private KafkaProperties kafkaProperties;

    @Mock
    private AvroDeserializer avroDeserializer;

    @Mock
    private DataProcessingService dataProcessingService;

    @InjectMocks
    private NetflowConsumer netflowConsumer;

    private static final String SCHEMA_JSON = """
            {
                "type": "record",
                "name": "NetFlowV2Record",
                "namespace": "org.nimbo.netflow.avro",
                "fields": [
                    {"name": "IPV4_SRC_ADDR", "type": "string"}
                ]
            }
            """;

    @BeforeEach
    void setUp() {
        lenient().when(kafkaProperties.getTopic()).thenReturn("test-topic");
        lenient().when(kafkaProperties.getPollTimeoutMs()).thenReturn(100);
        lenient().when(kafkaProperties.isEnableAutoCommit()).thenReturn(false);
    }

    private GenericRecord createTestRecord() {
        Schema schema = new Schema.Parser().parse(SCHEMA_JSON);
        GenericRecord record = new GenericData.Record(schema);
        record.put("IPV4_SRC_ADDR", "192.168.1.1");
        return record;
    }

    private Optional<Map<TopicPartitionKey, Long>> createOffsets() {
        TopicPartitionKey key = new TopicPartitionKey("test-topic", 0);
        return Optional.of(Map.of(key, 43L));
    }

    private Map<TopicPartitionKey, Long> createFlushOffsets() {
        TopicPartitionKey key = new TopicPartitionKey("test-topic", 0);
        return Map.of(key, 43L);
    }

    @Test
    @DisplayName("Should subscribe to configured topic on start")
    void shouldSubscribeToConfiguredTopicOnStart() {
        when(kafkaConsumer.poll(any(Duration.class)))
                .thenReturn(ConsumerRecords.empty());

        netflowConsumer.start();

        await().untilAsserted(() ->
                verify(kafkaConsumer).subscribe(Collections.singletonList("test-topic"))
        );
    }

    @Test
    @DisplayName("Should shutdown gracefully and close consumer")
    void shouldShutdownGracefullyAndCloseConsumer() {
        when(kafkaConsumer.poll(any(Duration.class)))
                .thenReturn(ConsumerRecords.empty());

        netflowConsumer.start();

        await().untilAsserted(() ->
                verify(kafkaConsumer).subscribe(anyCollection())
        );

        netflowConsumer.shutdown();

        verify(kafkaConsumer, timeout(2000)).close();
    }

    @Test
    @DisplayName("Should flush remaining records on shutdown")
    void shouldFlushRemainingRecordsOnShutdown() {
        when(kafkaConsumer.poll(any(Duration.class)))
                .thenReturn(ConsumerRecords.empty());

        Map<TopicPartitionKey, Long> flushOffsets = createFlushOffsets();
        when(dataProcessingService.flush()).thenReturn(flushOffsets);

        netflowConsumer.start();

        await().untilAsserted(() ->
                verify(kafkaConsumer).subscribe(anyCollection())
        );

        netflowConsumer.shutdown();

        verify(dataProcessingService).flush();
        verify(kafkaConsumer).commitSync(anyMap());
        verify(kafkaConsumer, timeout(2000)).close();
    }

    @Test
    @DisplayName("Should handle flush failure during shutdown gracefully")
    void shouldHandleFlushFailureDuringShutdown() {
        when(kafkaConsumer.poll(any(Duration.class)))
                .thenReturn(ConsumerRecords.empty());

        doThrow(new RuntimeException("Flush failed"))
                .when(dataProcessingService).flush();

        netflowConsumer.start();

        await().untilAsserted(() ->
                verify(kafkaConsumer).subscribe(anyCollection())
        );

        netflowConsumer.shutdown();

        verify(dataProcessingService).flush();
        verify(kafkaConsumer, timeout(2000)).close();
    }

    @Test
    @DisplayName("Should process single record successfully")
    void shouldProcessSingleRecordSuccessfully() {
        GenericRecord testRecord = createTestRecord();
        ConsumerRecord<byte[], byte[]> record = createConsumerRecord(0, 42L, 10);
        ConsumerRecords<byte[], byte[]> records = createConsumerRecords(record);

        when(kafkaConsumer.poll(any(Duration.class)))
                .thenReturn(records)
                .thenReturn(ConsumerRecords.empty());

        doNothing().when(kafkaConsumer).subscribe(anyCollection());
        when(avroDeserializer.deserialize(any(byte[].class))).thenReturn(testRecord);
        when(dataProcessingService.processRecord(any(IngestedRecord.class))).thenReturn(createOffsets());

        netflowConsumer.start();

        await().untilAsserted(() -> verify(dataProcessingService).processRecord(any(IngestedRecord.class)));
    }

    @Test
    @DisplayName("Should handle deserialization failure gracefully")
    void shouldHandleDeserializationFailureGracefully() {
        ConsumerRecord<byte[], byte[]> record = createConsumerRecord(0, 42L, 10);
        ConsumerRecords<byte[], byte[]> records = createConsumerRecords(record);

        when(kafkaConsumer.poll(any(Duration.class)))
                .thenReturn(records)
                .thenReturn(ConsumerRecords.empty());

        doNothing().when(kafkaConsumer).subscribe(anyCollection());
        when(avroDeserializer.deserialize(any(byte[].class)))
                .thenThrow(new RuntimeException("Deserialization failed"));

        netflowConsumer.start();

        await().untilAsserted(() -> {
            verify(avroDeserializer).deserialize(any(byte[].class));
            verify(dataProcessingService, never()).processRecord(any(IngestedRecord.class));
        });
    }

    @Test
    @DisplayName("Should continue polling after processing error")
    void shouldContinuePollingAfterProcessingError() {
        GenericRecord testRecord = createTestRecord();
        ConsumerRecord<byte[], byte[]> errorRecord = createConsumerRecord(0, 42L, 10);
        ConsumerRecord<byte[], byte[]> successRecord = createConsumerRecord(1, 43L, 10);

        ConsumerRecords<byte[], byte[]> errorRecords = createConsumerRecords(errorRecord);
        ConsumerRecords<byte[], byte[]> successRecords = createConsumerRecords(successRecord);

        when(kafkaConsumer.poll(any(Duration.class)))
                .thenReturn(errorRecords)
                .thenReturn(successRecords)
                .thenReturn(ConsumerRecords.empty());

        doNothing().when(kafkaConsumer).subscribe(anyCollection());
        when(avroDeserializer.deserialize(any(byte[].class)))
                .thenThrow(new RuntimeException("Processing failed"))
                .thenReturn(testRecord);
        when(dataProcessingService.processRecord(any(IngestedRecord.class))).thenReturn(createOffsets());

        netflowConsumer.start();

        await().untilAsserted(() -> verify(kafkaConsumer, atLeast(2)).poll(any(Duration.class)));
    }

    @Test
    @DisplayName("Should commit offsets when auto-commit is disabled")
    void shouldCommitOffsetsWhenAutoCommitDisabled() {
        GenericRecord testRecord = createTestRecord();
        ConsumerRecord<byte[], byte[]> record = createConsumerRecord(0, 42L, 10);
        ConsumerRecords<byte[], byte[]> records = createConsumerRecords(record);

        when(kafkaConsumer.poll(any(Duration.class)))
                .thenReturn(records)
                .thenReturn(ConsumerRecords.empty());

        doNothing().when(kafkaConsumer).subscribe(anyCollection());
        when(avroDeserializer.deserialize(any(byte[].class))).thenReturn(testRecord);
        when(dataProcessingService.processRecord(any(IngestedRecord.class))).thenReturn(createOffsets());

        netflowConsumer.start();

        await().untilAsserted(() -> verify(kafkaConsumer, atLeastOnce()).commitSync(anyMap()));
    }

    @Test
    @DisplayName("Should not commit offsets when auto-commit is enabled")
    void shouldNotCommitOffsetsWhenAutoCommitEnabled() {
        GenericRecord testRecord = createTestRecord();
        lenient().when(kafkaProperties.isEnableAutoCommit()).thenReturn(true);

        ConsumerRecord<byte[], byte[]> record = createConsumerRecord(0, 42L, 10);
        ConsumerRecords<byte[], byte[]> records = createConsumerRecords(record);

        when(kafkaConsumer.poll(any(Duration.class)))
                .thenReturn(records)
                .thenReturn(ConsumerRecords.empty());

        doNothing().when(kafkaConsumer).subscribe(anyCollection());
        when(avroDeserializer.deserialize(any(byte[].class))).thenReturn(testRecord);
        when(dataProcessingService.processRecord(any(IngestedRecord.class))).thenReturn(createOffsets());

        netflowConsumer.start();

        await().untilAsserted(() -> verify(kafkaConsumer, never()).commitSync(anyMap()));
    }

    @Test
    @DisplayName("Should not process empty poll results")
    void shouldNotProcessEmptyPollResults() {
        when(kafkaConsumer.poll(any(Duration.class)))
                .thenReturn(ConsumerRecords.empty());

        doNothing().when(kafkaConsumer).subscribe(anyCollection());

        netflowConsumer.start();

        await().untilAsserted(() -> {
            verify(kafkaConsumer, atLeastOnce()).poll(any(Duration.class));
            verify(dataProcessingService, never()).processRecord(any(IngestedRecord.class));
        });
    }

    @Test
    @DisplayName("Should handle consumer subscription failure")
    void shouldHandleConsumerSubscriptionFailure() {
        doThrow(new RuntimeException("Subscription failed"))
                .when(kafkaConsumer).subscribe(anyCollection());

        netflowConsumer.start();

        await().pollDelay(Duration.ofMillis(100))
                .atMost(Duration.ofSeconds(5))
                .until(() -> {
                    for (Thread t : Thread.getAllStackTraces().keySet()) {
                        if (t.getName().equals("netflow-consumer-thread")) {
                            return !t.isAlive();
                        }
                    }
                    return true;
                });

        verify(kafkaConsumer, atLeastOnce()).close();
    }

    @Test
    @DisplayName("Should process multiple records from different partitions")
    void shouldProcessMultipleRecordsFromDifferentPartitions() {
        GenericRecord testRecord = createTestRecord();
        ConsumerRecord<byte[], byte[]> record1 = createConsumerRecord(0, 42L, 10);
        ConsumerRecord<byte[], byte[]> record2 = createConsumerRecord(1, 43L, 15);
        ConsumerRecord<byte[], byte[]> record3 = createConsumerRecord(2, 44L, 20);

        TopicPartition partition0 = new TopicPartition("test-topic", 0);
        TopicPartition partition1 = new TopicPartition("test-topic", 1);
        TopicPartition partition2 = new TopicPartition("test-topic", 2);

        Map<TopicPartition, java.util.List<ConsumerRecord<byte[], byte[]>>> recordMap = Map.of(
                partition0, java.util.List.of(record1),
                partition1, java.util.List.of(record2),
                partition2, java.util.List.of(record3)
        );

        ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<>(recordMap);

        when(kafkaConsumer.poll(any(Duration.class)))
                .thenReturn(records)
                .thenReturn(ConsumerRecords.empty());

        doNothing().when(kafkaConsumer).subscribe(anyCollection());
        when(avroDeserializer.deserialize(any(byte[].class))).thenReturn(testRecord);
        when(dataProcessingService.processRecord(any(IngestedRecord.class))).thenReturn(createOffsets());

        netflowConsumer.start();

        await().untilAsserted(() -> verify(dataProcessingService, times(3)).processRecord(any(IngestedRecord.class)));
    }

    @Test
    @DisplayName("Should handle interruption during shutdown")
    void shouldHandleInterruptionDuringShutdown() {
        AtomicReference<Thread> consumerThread = new AtomicReference<>();

        doAnswer(invocation -> {
            consumerThread.set(Thread.currentThread());
            return null;
        }).when(kafkaConsumer).subscribe(anyCollection());

        when(kafkaConsumer.poll(any(Duration.class)))
                .thenAnswer(invocation -> {
                    Thread.sleep(1000);
                    return ConsumerRecords.empty();
                });

        netflowConsumer.start();

        await().untilAsserted(() -> assertThat(consumerThread.get()).isNotNull());

        consumerThread.get().interrupt();
        netflowConsumer.shutdown();

        verify(kafkaConsumer, timeout(2000)).close();
    }

    private ConsumerRecord<byte[], byte[]> createConsumerRecord(int partition, long offset, int valueSize) {
        return new ConsumerRecord<>("test-topic", partition, offset, new byte[0], new byte[valueSize]);
    }

    private ConsumerRecords<byte[], byte[]> createConsumerRecords(ConsumerRecord<byte[], byte[]> record) {
        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
        return new ConsumerRecords<>(Map.of(partition, java.util.List.of(record)));
    }
}