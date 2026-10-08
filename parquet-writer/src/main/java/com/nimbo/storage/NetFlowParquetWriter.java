package com.nimbo.storage;

import com.nimbo.config.HdfsProperties;
import com.nimbo.metrics.ParquetWriterMetrics;
import com.nimbo.model.IngestedRecord;
import com.nimbo.model.TopicPartitionKey;
import com.nimbo.storage.batching.BatchAccumulator;
import com.nimbo.storage.batching.BatchPolicy;
import com.nimbo.storage.exception.StorageException;
import com.nimbo.storage.hdfs.FileStorage;
import com.nimbo.storage.naming.FileNameGenerator;
import com.nimbo.storage.parquet.ParquetFileWriter;
import com.nimbo.storage.schema.StorageSchemaEvolution;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

@Slf4j(topic = "com.nimbo.storage.NetFlowStorage")
public final class NetFlowParquetWriter {

  private final ParquetFileLifecycle parquetFileLifecycle;
  private final HourWindowPolicy hourWindowPolicy;
  private final StorageSchemaEvolution schemaEvolution = new StorageSchemaEvolution();
  private final ParquetRecordMapper recordMapper = new ParquetRecordMapper(schemaEvolution);

  private final BatchAccumulator batchAccumulator;
  private final Map<Instant, ParquetFileLifecycle.WriterState> writers = new HashMap<>();

  private final ParquetWriterMetrics metrics;
  private final int instanceId;
  private Schema knownSourceSchema;
  private final Set<Schema> knownSchemas = new HashSet<>();

  public NetFlowParquetWriter(
      BatchPolicy batchPolicy,
      ParquetFileWriter parquetFileWriter,
      FileStorage fileStorage,
      FileNameGenerator fileNameGenerator,
      HdfsProperties hdfsProperties,
      HourWindowPolicy hourWindowPolicy,
      ParquetWriterMetrics metrics,
      int instanceId,
      List<Schema> registeredSchemas) {
    this.parquetFileLifecycle =
        new ParquetFileLifecycle(
            parquetFileWriter, fileStorage, fileNameGenerator, hdfsProperties);
    this.hourWindowPolicy = Objects.requireNonNull(hourWindowPolicy);
    this.batchAccumulator = new BatchAccumulator(batchPolicy);
    this.metrics = Objects.requireNonNull(metrics);
    this.instanceId = instanceId;
    initializeSchemas(registeredSchemas);

    metrics.registerGauge(
        "ingester_open_batch_record_count",
        instanceId,
        batchAccumulator,
        BatchAccumulator::getRecordCount);

    metrics.registerGauge(
        "ingester_open_batch_age_seconds",
        instanceId,
        this,
        NetFlowParquetWriter::currentBatchAgeSeconds);

    log.info(
        "NetFlowStorage initialized with {} registered schema version(s) and HDFS target path: {}",
        knownSchemas.size(),
        hdfsProperties.getBasePath());
  }

  private void initializeSchemas(List<Schema> registeredSchemas) {
    Objects.requireNonNull(registeredSchemas, "registeredSchemas must not be null");
    if (registeredSchemas.isEmpty()) {
      throw new IllegalArgumentException("registeredSchemas must not be empty");
    }

    for (Schema registeredSchema : registeredSchemas) {
      knownSourceSchema = schemaEvolution.merge(knownSourceSchema, registeredSchema);
      knownSchemas.add(registeredSchema);
    }
  }

  private double currentBatchAgeSeconds() {
    if (writers.isEmpty()) {
      return 0;
    }
    Instant firstRecordAt = batchAccumulator.getFirstRecordAt();
    return firstRecordAt == null ? 0 : Duration.between(firstRecordAt, Instant.now()).getSeconds();
  }

  public Optional<Map<TopicPartitionKey, Long>> accept(IngestedRecord ingestedRecord) {

    Objects.requireNonNull(ingestedRecord, "ingestedRecord must not be null");

    try {
      Instant recordBucket = HourWindowPolicy.hourOf(ingestedRecord.kafkaTimestamp());
      Map<TopicPartitionKey, Long> boundaryOffsets = Map.of();
      Schema recordSchema = ingestedRecord.record().getSchema();
      boolean unseenSchema = !knownSchemas.contains(recordSchema);
      Schema mergedSchema = knownSourceSchema;
      boolean schemaChanged = false;

      if (unseenSchema) {
        mergedSchema = schemaEvolution.merge(knownSourceSchema, recordSchema);
        schemaChanged = knownSourceSchema != null && !mergedSchema.equals(knownSourceSchema);
      }

      if (!writers.isEmpty() && schemaChanged) {
        log.info(
            "Batch flush triggered: reason={}, recordCount={},"
                + " accumulatedSizeBytes={}, firstRecordAt={}",
            BatchPolicy.FlushReason.SCHEMA_CHANGE,
            batchAccumulator.getRecordCount(),
            batchAccumulator.getAccumulatedSizeBytes(),
            batchAccumulator.getFirstRecordAt());

        boundaryOffsets =
            mergeOffsets(
                boundaryOffsets, commitOpenWriters(BatchPolicy.FlushReason.SCHEMA_CHANGE));
      }

      knownSourceSchema = mergedSchema;
      if (unseenSchema) {
        knownSchemas.add(recordSchema);
      }

      ParquetFileLifecycle.WriterState writerState = writers.get(recordBucket);
      if (writerState == null) {
        if (writers.size() >= hourWindowPolicy.maxOpenWriters()) {
          boundaryOffsets =
              mergeOffsets(
                  boundaryOffsets,
                  commitOpenWriters(BatchPolicy.FlushReason.TIME_BUCKET_BOUNDARY));
        }
        writerState = openWriter(ingestedRecord);
        writers.put(recordBucket, writerState);
      }

      GenericRecord enrichedRecord = recordMapper.enrichWithKafkaMetadata(ingestedRecord, writerState.storageSchema());

      parquetFileLifecycle.write(writerState, enrichedRecord);
      metrics.recordWritten(instanceId);
      batchAccumulator.add(ingestedRecord);

      if (batchAccumulator.shouldFlush()) {
        Map<TopicPartitionKey, Long> offsets = commitOpenWriters(batchAccumulator.flushReason());
        return Optional.of(mergeOffsets(boundaryOffsets, offsets));
      }

      return boundaryOffsets.isEmpty() ? Optional.empty() : Optional.of(boundaryOffsets);

    } catch (StorageException e) {
      handleWriteFailure();
      log.error(
          "Failed writing record: topic={}, partition={}, offset={}",
          ingestedRecord.topic(),
          ingestedRecord.partition(),
          ingestedRecord.offset(),
          e);
      throw e;
    }
  }

  private static Map<TopicPartitionKey, Long> mergeOffsets(
      Map<TopicPartitionKey, Long> a, Map<TopicPartitionKey, Long> b) {
    if (a.isEmpty()) {
      return b;
    }
    if (b.isEmpty()) {
      return a;
    }
    Map<TopicPartitionKey, Long> merged = new HashMap<>(a);
    b.forEach((k, v) -> merged.merge(k, v, Math::max));
    return merged;
  }

  public Map<TopicPartitionKey, Long> flush() {
    if (writers.isEmpty()) {
      log.debug("Flush requested but accumulator is empty");
      return Map.of();
    }

    log.info("Executing lifecycle flush for {} records", batchAccumulator.getRecordCount());

    try {
      return commitOpenWriters(BatchPolicy.FlushReason.LIFECYCLE);
    } catch (StorageException e) {
      handleWriteFailure();
      throw e;
    }
  }

  public Map<TopicPartitionKey, Long> flushRemaining() {
    if (writers.isEmpty()) {
      return Map.of();
    }

    if (!batchAccumulator.shouldFlush()) {
      return Map.of();
    }

    log.info(
        "Flushed remaining records. Record count: {}, accumulated size: {} bytes",
        batchAccumulator.getRecordCount(),
        batchAccumulator.getAccumulatedSizeBytes());

    try {
      return commitOpenWriters(batchAccumulator.flushReason());
    } catch (StorageException e) {
      handleWriteFailure();
      throw e;
    }
  }

  public Optional<Map<TopicPartitionKey, Long>> recordSkippedOffset(
      String topic, int partition, long nextOffset) {
    if (writers.isEmpty()) {
      return Optional.of(Map.of(new TopicPartitionKey(topic, partition), nextOffset));
    }
    return batchAccumulator.recordSkippedOffset(topic, partition, nextOffset);
  }

  public boolean isWithinAllowedWindow(long kafkaTimestampMillis) {
    return hourWindowPolicy.includes(kafkaTimestampMillis);
  }

  private ParquetFileLifecycle.WriterState openWriter(IngestedRecord firstRecord) {
    return parquetFileLifecycle.openWriter(firstRecord, knownSourceSchema, recordMapper);
  }

  private Map<TopicPartitionKey, Long> commitOpenWriters(BatchPolicy.FlushReason reason) {
    return parquetFileLifecycle.commitOpenWriters(writers, batchAccumulator, reason, metrics, instanceId);
  }

  private void handleWriteFailure() {
    parquetFileLifecycle.handleWriteFailure(writers, batchAccumulator, metrics, instanceId);
  }
}
