package com.nimbo.starrockswriter.processing;

import com.nimbo.common.validation.NetflowValidationService;
import com.nimbo.common.validation.ValidationResult;
import com.nimbo.starrockswriter.avro.AvroDeserializer;
import com.nimbo.starrockswriter.streamload.StreamLoadRetryHandler;
import com.nimbo.starrockswriter.streamload.StreamLoadLabelGenerator;
import com.nimbo.starrockswriter.mapper.StarRocksRecordMapper;
import com.nimbo.starrockswriter.model.PendingLoad;
import com.nimbo.starrockswriter.model.StarRocksRow;
import com.nimbo.starrockswriter.model.StreamLoadResult;
import com.nimbo.starrockswriter.metrics.StarRocksWriterMetrics;
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
    private final StarRocksWriterMetrics metrics;

    @Getter
    private boolean kafkaPaused;

    public void processRecords(ConsumerRecords<byte[], byte[]> records) {

        for (ConsumerRecord<byte[], byte[]> consumerRecord : records) {
            try {
                processRecord(consumerRecord);
            } catch (Exception e) {
                if (!(e instanceof AccountedProcessingException)) {
                    metrics.recordProcessed(workerNumber, "processing_error");
                }
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

    private void processRecord(ConsumerRecord<byte[], byte[]> consumerRecord) {
        GenericRecord avroRecord;
        try {
            avroRecord = avroDeserializer.deserialize(consumerRecord.value());
        } catch (RuntimeException exception) {
            metrics.recordProcessed(workerNumber, "deserialization_error");
            throw new AccountedProcessingException(exception);
        }
        ValidationResult validation = validationService.validate(avroRecord);
        TopicPartition partition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
        OffsetAndMetadata offset = new OffsetAndMetadata(consumerRecord.offset() + 1);

        if (!validation.valid()) {
            metrics.recordProcessed(workerNumber, "validation_error");
            batchBuffer.addOffset(partition, offset);
            return;
        }

        StarRocksRow row;
        try {
            row = mapper.map(avroRecord, consumerRecord.topic(), consumerRecord.partition(),
                    consumerRecord.offset(), consumerRecord.timestamp());
        } catch (RuntimeException exception) {
            metrics.recordProcessed(workerNumber, "mapping_error");
            throw new AccountedProcessingException(exception);
        }

        batchBuffer.add(row, partition, offset);
        metrics.recordProcessed(workerNumber, "success");
        updateBatchMetrics();
    }

    public void flushIfNeeded() {
        updateBatchMetrics();
        BatchBuffer.FlushReason reason = batchBuffer.flushReason();
        if (reason == null) return;
        submitBatch(reason);
    }

    private void submitBatch(BatchBuffer.FlushReason reason) {
        if (batchBuffer.isEmpty()) {
            metrics.emptyBatch();
            return;
        }

        BatchBuffer.BatchSnapshot batchSnapshot = batchBuffer.snapshot();
        String label = labelGenerator.generateLabel(workerNumber, batchSnapshot.offsets());
        CompletableFuture<StreamLoadResult> future = retryHandler.sendRequest(batchSnapshot.rows(), label);
        PendingLoad pendingLoad = new PendingLoad(label, batchSnapshot.rows().size(), batchSnapshot.offsets(), future);

        inFlightLoadManager.add(pendingLoad);
        metrics.batchSubmitted(workerNumber, reason.name().toLowerCase(), batchSnapshot.rows().size());
        batchBuffer.clear();
        updateBatchMetrics();

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
            submitBatch(BatchBuffer.FlushReason.SHUTDOWN);
        }
        inFlightLoadManager.drain(consumer);
    }

    private void updateBatchMetrics() {
        metrics.setOpenBatch(workerNumber, batchBuffer.rowCount(), batchBuffer.ageSeconds());
    }

    private static final class AccountedProcessingException extends RuntimeException {
        private AccountedProcessingException(RuntimeException cause) {
            super(cause);
        }
    }
}
