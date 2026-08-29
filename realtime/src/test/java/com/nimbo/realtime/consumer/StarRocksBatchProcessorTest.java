package com.nimbo.realtime.consumer;

import com.nimbo.realtime.avro.AvroDeserializer;
import com.nimbo.realtime.client.StarRocksStreamLoadClient;
import com.nimbo.realtime.config.StarRocksProperties;
import com.nimbo.realtime.mapper.StarRocksRecordMapper;
import com.nimbo.realtime.model.StarRocksRow;
import com.nimbo.realtime.model.StreamLoadResult;
import com.nimbo.realtime.validation.NetflowValidationService;
import com.nimbo.realtime.validation.ValidationResult;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StarRocksBatchProcessorTest {

    private AvroDeserializer avroDeserializer;
    private NetflowValidationService validationService;
    private StarRocksRecordMapper mapper;
    private StarRocksStreamLoadClient streamLoadClient;
    private StarRocksProperties properties;
    private StarRocksBatchProcessor processor;

    @BeforeEach
    void setUp() {
        avroDeserializer = mock(AvroDeserializer.class);
        validationService = mock(NetflowValidationService.class);
        mapper = mock(StarRocksRecordMapper.class);
        streamLoadClient = mock(StarRocksStreamLoadClient.class);

        properties = new StarRocksProperties();
        properties.setMaxBatchSizeRows(1);
        properties.setMaxBatchTimeMs(60_000);
        properties.setMaxInFlightRequests(1);
        properties.setRetryBackoffMs(100);

        processor = new StarRocksBatchProcessor(
                3, properties, avroDeserializer, validationService, mapper, streamLoadClient
        );
    }

    @Test
    void validRecordIsLoadedAndItsNextOffsetIsCommitted() {
        GenericRecord avroRecord = mock(GenericRecord.class);
        StarRocksRow row = new StarRocksRow("netflow", 2, 10, 1234, List.of("value"));
        StreamLoadResult success = result("Success");

        when(avroDeserializer.deserialize(any())).thenReturn(avroRecord);
        when(validationService.validate(avroRecord)).thenReturn(ValidationResult.success());
        when(mapper.map(avroRecord, "netflow", 2, 10L, 1234L)).thenReturn(row);
        when(streamLoadClient.loadAsync(eq(List.of(row)), eq("nimbo-w3-p2-o11")))
                .thenReturn(CompletableFuture.completedFuture(success));

        processor.process(records(record(2, 10L, 1234L)));
        processor.flushIfNeeded();

        @SuppressWarnings("unchecked")
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        processor.drainCompletedLoads(consumer);

        verify(streamLoadClient).loadAsync(List.of(row), "nimbo-w3-p2-o11");
        verify(consumer).commitSync(Map.of(
                new TopicPartition("netflow", 2), new OffsetAndMetadata(11L)
        ));
    }

    @Test
    void invalidRecordIsNotSentToStarRocks() {
        GenericRecord avroRecord = mock(GenericRecord.class);
        when(avroDeserializer.deserialize(any())).thenReturn(avroRecord);
        when(validationService.validate(avroRecord)).thenReturn(ValidationResult.failure("bad record"));

        processor.process(records(record(0, 5L, 100L)));
        processor.flushIfNeeded();

        verify(streamLoadClient, never()).loadAsync(any(), any());
    }

    @Test
    void backpressureAndPauseFlagsReflectProcessorState() {
        assertFalse(processor.isKafkaPaused());
        processor.markKafkaPaused();
        assertTrue(processor.isKafkaPaused());
        processor.markKafkaResumed();
        assertFalse(processor.isKafkaPaused());
    }

    @Test
    void publishTimeoutIsAcceptedWithoutRetry() {
        GenericRecord avroRecord = mock(GenericRecord.class);
        StarRocksRow row = new StarRocksRow("netflow", 0, 1, 100, List.of("value"));

        when(avroDeserializer.deserialize(any())).thenReturn(avroRecord);
        when(validationService.validate(avroRecord)).thenReturn(ValidationResult.success());
        when(mapper.map(avroRecord, "netflow", 0, 1L, 100L)).thenReturn(row);
        when(streamLoadClient.loadAsync(eq(List.of(row)), eq("nimbo-w3-p0-o2")))
                .thenReturn(CompletableFuture.completedFuture(result("Publish Timeout")));

        processor.process(records(record(0, 1L, 100L)));
        processor.flushIfNeeded();

        @SuppressWarnings("unchecked")
        KafkaConsumer<byte[], byte[]> consumer = mock(KafkaConsumer.class);
        processor.drainCompletedLoads(consumer);

        verify(streamLoadClient).loadAsync(List.of(row), "nimbo-w3-p0-o2");
        verify(consumer).commitSync(any(Map.class));
    }

    @SuppressWarnings("unchecked")
    private ConsumerRecord<byte[], byte[]> record(int partition, long offset, long timestamp) {
        ConsumerRecord<byte[], byte[]> record = mock(ConsumerRecord.class);
        when(record.topic()).thenReturn("netflow");
        when(record.partition()).thenReturn(partition);
        when(record.offset()).thenReturn(offset);
        when(record.timestamp()).thenReturn(timestamp);
        when(record.value()).thenReturn(new byte[]{1, 2, 3});
        return record;
    }

    private ConsumerRecords<byte[], byte[]> records(ConsumerRecord<byte[], byte[]> record) {
        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
        return new ConsumerRecords<>(Map.of(partition, List.of(record)));
    }

    private StreamLoadResult result(String status) {
        return new StreamLoadResult(
                1L, "label", status, null,
                1L, 1L, 0L, 10L,
                1L, 1L, 1L, 1L,
                1L, 1L
        );
    }
}
