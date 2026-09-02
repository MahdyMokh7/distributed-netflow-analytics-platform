package com.nimbo.realtime.processing;

import com.nimbo.realtime.avro.AvroDeserializer;
import com.nimbo.realtime.streamload.StreamLoadRetryHandler;
import com.nimbo.realtime.streamload.StreamLoadLabelGenerator;
import com.nimbo.realtime.mapper.StarRocksRecordMapper;
import com.nimbo.realtime.model.PendingLoad;
import com.nimbo.realtime.model.StarRocksRow;
import com.nimbo.realtime.model.StreamLoadResult;
import com.nimbo.realtime.validation.NetflowValidationService;
import com.nimbo.realtime.validation.ValidationResult;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.concurrent.CompletableFuture;

@Slf4j
@RequiredArgsConstructor
public class BatchProcessor {

    private final int workerNumber;
    private final AvroDeserializer avroDeserializer;
    private final NetflowValidationService validationService;
    private final StarRocksRecordMapper mapper;
    private final BatchBuffer batchBuffer;
    private final InFlightLoadManager inFlightLoadManager;
    private final StreamLoadRetryHandler retryHandler;
    private final StreamLoadLabelGenerator labelGenerator;

    @Getter
    private boolean kafkaPaused;

    public void processRecords(ConsumerRecords<byte[], byte[]> records) {

        for (ConsumerRecord<byte[], byte[]> consumerRecord : records) {
            try {
                processRecord(consumerRecord);
            } catch (Exception e) {
                log.error(
                        "Failed processing record topic={}, partition={}, offset={}",
                        consumerRecord.topic(), consumerRecord.partition(),
                        consumerRecord.offset(), e
                );
                batchBuffer.addOffset(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                        new OffsetAndMetadata(consumerRecord.offset() + 1));
            }
        }
    }

    private void processRecord(ConsumerRecord<byte[], byte[]> kafkaRecord) {
        GenericRecord avroRecord = avroDeserializer.deserialize(kafkaRecord.value());
        ValidationResult validation = validationService.validate(avroRecord);
        TopicPartition partition = new TopicPartition(kafkaRecord.topic(), kafkaRecord.partition());
        OffsetAndMetadata offset = new OffsetAndMetadata(kafkaRecord.offset() + 1);

        if (!validation.valid()) {
            batchBuffer.addOffset(partition, offset);
            return;
        }

        StarRocksRow row = mapper.map(avroRecord, kafkaRecord.topic(), kafkaRecord.partition(),
                kafkaRecord.offset(), kafkaRecord.timestamp());

        batchBuffer.add(row, partition, offset);
    }

    public void flushIfNeeded() {
        if (!batchBuffer.shouldFlush()) return;
        submitBatch();
    }

    private void submitBatch() {
        if (batchBuffer.isEmpty()) return;

        BatchBuffer.BatchSnapshot batchSnapshot = batchBuffer.snapshot();
        String label = labelGenerator.generateLabel(workerNumber, batchSnapshot.offsets());
        CompletableFuture<StreamLoadResult> future = retryHandler.sendRequest(batchSnapshot.rows(), label);
        PendingLoad pendingLoad = new PendingLoad(label, batchSnapshot.rows().size(), batchSnapshot.offsets(), future);

        inFlightLoadManager.add(pendingLoad);
        batchBuffer.clear();

        log.debug("Worker {} submitted batch. label={}, rows={}", workerNumber, label, batchSnapshot.rows().size());
    }

    public void drainCompletedLoads(KafkaConsumer<byte[], byte[]> consumer) {
        inFlightLoadManager.drain(consumer);
    }

    public boolean hasTooManyInFlightLoads() {
        return inFlightLoadManager.isBackpressured();
    }

    public void markKafkaPaused() {
        kafkaPaused = true;
    }

    public void markKafkaResumed() {
        kafkaPaused = false;
    }

    public void shutdown(KafkaConsumer<byte[], byte[]> consumer) {
        if (!batchBuffer.isEmpty()) {
            submitBatch();
        }
        inFlightLoadManager.drain(consumer);
    }
}
