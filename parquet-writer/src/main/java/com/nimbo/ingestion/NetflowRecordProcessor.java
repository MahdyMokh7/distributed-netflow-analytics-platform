package com.nimbo.ingestion;

import com.nimbo.common.validation.NetflowValidationService;
import com.nimbo.common.validation.ValidationResult;
import com.nimbo.ingestion.exception.DeserializationException;
import com.nimbo.metrics.ParquetWriterMetrics;
import com.nimbo.model.IngestedRecord;
import com.nimbo.model.TopicPartitionKey;
import com.nimbo.storage.exception.UnsupportedSchemaException;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public final class NetflowRecordProcessor {

  private final AvroDeserializer avroDeserializer;
  private final DataProcessingService dataProcessingService;
  private final NetflowValidationService validationService;
  private final DlqService dlqService;
  private final int instanceId;
  private final ParquetWriterMetrics metrics;

  public NetflowRecordProcessor(
      AvroDeserializer avroDeserializer,
      DataProcessingService dataProcessingService,
      NetflowValidationService validationService,
      DlqService dlqService,
      int instanceId,
      ParquetWriterMetrics metrics) {
    this.avroDeserializer = avroDeserializer;
    this.dataProcessingService = dataProcessingService;
    this.validationService = validationService;
    this.dlqService = dlqService;
    this.instanceId = instanceId;
    this.metrics = metrics;
  }

  public Optional<Map<TopicPartitionKey, Long>> processRecord(ConsumerRecord<byte[], byte[]> consumerRecord) {
    long nextOffset = consumerRecord.offset() + 1;

    if (!dataProcessingService.isWithinAllowedWindow(consumerRecord.timestamp())) {
      metrics.invalidRecord(instanceId, DlqReason.LATE_RECORD);
      dlqService.publishLate(consumerRecord);
      return dataProcessingService.recordSkippedOffset(consumerRecord.topic(), consumerRecord.partition(), nextOffset);
    }

    try {
      var genericRecord = avroDeserializer.deserialize(consumerRecord.topic(), consumerRecord.value());
      ValidationResult result = validationService.validate(genericRecord);

      if (!result.valid()) {
        metrics.invalidRecord(instanceId, DlqReason.VALIDATION_ERROR);
        dlqService.publish(consumerRecord, DlqReason.VALIDATION_ERROR, result.reason());
        return dataProcessingService.recordSkippedOffset(
            consumerRecord.topic(), consumerRecord.partition(), nextOffset);
      }

      IngestedRecord ingestedRecord = new IngestedRecord(
          genericRecord,
          consumerRecord.topic(),
          consumerRecord.partition(),
          consumerRecord.offset(),
          consumerRecord.timestamp(),
          consumerRecord.value().length);

      return dataProcessingService.processRecord(ingestedRecord);

    } catch (DeserializationException | UnsupportedSchemaException e) {
      DlqReason reason = e instanceof DeserializationException
          ? DlqReason.DESERIALIZATION_ERROR
          : DlqReason.UNSUPPORTED_SCHEMA;
      metrics.invalidRecord(instanceId, reason);
      dlqService.publish(consumerRecord, reason, e.getMessage());

      return dataProcessingService.recordSkippedOffset(consumerRecord.topic(), consumerRecord.partition(), nextOffset);

    }
  }
}
