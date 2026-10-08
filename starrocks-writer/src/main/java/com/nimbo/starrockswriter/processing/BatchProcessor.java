package com.nimbo.starrockswriter.processing;

import com.nimbo.common.validation.NetflowValidationService;
import com.nimbo.common.validation.ValidationResult;
import com.nimbo.starrockswriter.mapper.StarRocksRecordMapper;
import com.nimbo.starrockswriter.metrics.StarRocksWriterMetrics;
import com.nimbo.starrockswriter.model.PendingLoad;
import com.nimbo.starrockswriter.model.StarRocksRow;
import com.nimbo.starrockswriter.model.StreamLoadResult;
import com.nimbo.starrockswriter.schema.StarRocksSchemaEvolutionManager;
import com.nimbo.starrockswriter.streamload.StreamLoadLabelGenerator;
import com.nimbo.starrockswriter.streamload.StreamLoadRetryHandler;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

@Slf4j
@RequiredArgsConstructor
public class BatchProcessor {

  private final int workerNumber;
  private final NetflowValidationService validationService;
  private final StarRocksRecordMapper mapper;
  private final StarRocksSchemaEvolutionManager schemaEvolutionManager;
  private final BatchBuffer batchBuffer;
  private final InFlightLoadManager inFlightLoadManager;
  private final StreamLoadRetryHandler retryHandler;
  private final StreamLoadLabelGenerator labelGenerator;
  private final StarRocksWriterMetrics metrics;

  @Getter
  private boolean kafkaPaused;

  public void processRecords(ConsumerRecords<byte[], GenericRecord> records) {

    for (ConsumerRecord<byte[], GenericRecord> consumerRecord : records) {
      try {
        processRecord(consumerRecord);
      } catch (NonSkippableProcessingException exception) {
        log.error("Non-skippable processing failure; offset will not be advanced: topic={}, partition={}, offset={}",
            consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), exception);
        throw exception;
      } catch (RuntimeException exception) {
        if (!(exception instanceof AccountedProcessingException)) {
          metrics.recordProcessed(workerNumber, "processing_error");
        }
        log.error(
            "Skipping record after record-local processing failure: topic={}, partition={}," + " offset={}",
            consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), exception);
        batchBuffer.addOffset(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
            new OffsetAndMetadata(consumerRecord.offset() + 1));
      }
    }
  }

  private void processRecord(ConsumerRecord<byte[], GenericRecord> consumerRecord) {
    GenericRecord avroRecord = consumerRecord.value();
    if (avroRecord == null) {
      metrics.recordProcessed(workerNumber, "deserialization_error");
      throw new AccountedProcessingException(
          new IllegalArgumentException("Kafka record value is null; tombstones are not supported"));
    }

    try {
      schemaEvolutionManager.ensureSchema(avroRecord.getSchema());
    } catch (RuntimeException exception) {
      metrics.recordProcessed(workerNumber, "schema_evolution_error");
      throw new NonSkippableProcessingException(exception);
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
      row = mapper.map(avroRecord, consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(),
              consumerRecord.timestamp());
    } catch (RuntimeException exception) {
      metrics.recordProcessed(workerNumber, "mapping_error");
      throw new AccountedProcessingException(exception);
    }

    batchBuffer.add(row, partition, offset);
    metrics.recordProcessed(workerNumber, "success");
  }

  public void skipDeserializationFailure(TopicPartition partition, long nextOffset) {
    batchBuffer.addOffset(partition, new OffsetAndMetadata(nextOffset));
    submitBatch(BatchBuffer.FlushReason.DESERIALIZATION_ERROR);
  }

  public void flushIfNeeded() {
    BatchBuffer.FlushReason reason = batchBuffer.flushReason();
    if (reason == null) {
      return;
    }
    submitBatch(reason);
  }

  private void submitBatch(BatchBuffer.FlushReason reason) {
    if (batchBuffer.isEmpty()) {
      return;
    }

    BatchBuffer.BatchSnapshot batchSnapshot = batchBuffer.snapshot();
    String label = labelGenerator.generateLabel(workerNumber, batchSnapshot.offsets());
    CompletableFuture<StreamLoadResult> future = batchSnapshot.rows().isEmpty()
            ? CompletableFuture.completedFuture(StreamLoadResult.offsetOnly(label))
            : retryHandler.sendRequest(batchSnapshot.rows(), label);
    PendingLoad pendingLoad = new PendingLoad(label, batchSnapshot.rows().size(), batchSnapshot.offsets(), future);

    inFlightLoadManager.add(pendingLoad);
    metrics.batchSubmitted(workerNumber, batchSnapshot.rows().size());
    batchBuffer.clear();

    log.debug("Worker {} submitted batch. reason={}, label={}, rows={}", workerNumber, reason, label,
        batchSnapshot.rows().size());
  }

  public void drainCompletedLoads(KafkaConsumer<byte[], GenericRecord> consumer) {
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

  public void prepareForRebalance(KafkaConsumer<byte[], GenericRecord> consumer) {
    if (!batchBuffer.isEmpty()) {
      submitBatch(BatchBuffer.FlushReason.REBALANCE);
    }
    inFlightLoadManager.awaitAndDrain(consumer);
  }

  public void shutdown(KafkaConsumer<byte[], GenericRecord> consumer) {
    if (!batchBuffer.isEmpty()) {
      submitBatch(BatchBuffer.FlushReason.SHUTDOWN);
    }
    inFlightLoadManager.drain(consumer);
  }

  private static final class AccountedProcessingException extends RuntimeException {

    private AccountedProcessingException(RuntimeException cause) {
      super(cause);
    }
  }

  private static final class NonSkippableProcessingException extends RuntimeException {

    private NonSkippableProcessingException(RuntimeException cause) {
      super(cause);
    }
  }
}
