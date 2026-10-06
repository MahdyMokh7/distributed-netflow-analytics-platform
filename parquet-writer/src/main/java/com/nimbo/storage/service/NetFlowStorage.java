package com.nimbo.storage.service;

import com.nimbo.ingester.model.IngestedRecord;
import com.nimbo.metrics.ParquetWriterMetrics;
import com.nimbo.storage.config.HdfsProperties;
import com.nimbo.storage.exception.ParquetWriteException;
import com.nimbo.storage.exception.StorageException;
import com.nimbo.storage.model.TopicPartitionKey;
import com.nimbo.storage.service.batching.BatchAccumulator;
import com.nimbo.storage.service.batching.BatchPolicy;
import com.nimbo.storage.service.hdfs.FileStorage;
import com.nimbo.storage.service.naming.FileNameGenerator;
import com.nimbo.storage.service.parquet.ParquetFileWriter;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;

@Slf4j
public final class NetFlowStorage {

  private final ParquetFileWriter parquetFileWriter;
  private final FileStorage fileStorage;
  private final FileNameGenerator fileNameGenerator;
  private final HdfsProperties hdfsProperties;
  private final StorageSchemaEvolution schemaEvolution = new StorageSchemaEvolution();

  private final BatchAccumulator batchAccumulator;

  private ParquetWriter<GenericRecord> currentWriter;
  private Path currentTempPath;
  private Path currentFinalPath;
  private boolean writerOpen = false;

  private final ParquetWriterMetrics metrics;
  private final int instanceId;
  private Instant currentBucketHour;
  private Schema knownSourceSchema;
  private Schema currentStorageSchema;
  private final Set<Schema> knownSchemas = new HashSet<>();

  public NetFlowStorage(
      BatchPolicy batchPolicy,
      ParquetFileWriter parquetFileWriter,
      FileStorage fileStorage,
      FileNameGenerator fileNameGenerator,
      HdfsProperties hdfsProperties,
      ParquetWriterMetrics metrics,
      int instanceId,
      List<Schema> registeredSchemas) {
    this.parquetFileWriter = Objects.requireNonNull(parquetFileWriter);
    this.fileStorage = Objects.requireNonNull(fileStorage);
    this.fileNameGenerator = Objects.requireNonNull(fileNameGenerator);
    this.hdfsProperties = Objects.requireNonNull(hdfsProperties);
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
        NetFlowStorage::currentBatchAgeSeconds);

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
    if (!writerOpen) {
      return 0;
    }
    Instant firstRecordAt = batchAccumulator.getFirstRecordAt();
    return firstRecordAt == null ? 0 : Duration.between(firstRecordAt, Instant.now()).getSeconds();
  }

  private static Instant hourBucket(long kafkaTimestampMillis) {
    return Instant.ofEpochMilli(kafkaTimestampMillis).truncatedTo(ChronoUnit.HOURS);
  }

  public Optional<Map<TopicPartitionKey, Long>> accept(IngestedRecord ingestedRecord) {

    Objects.requireNonNull(ingestedRecord, "ingestedRecord must not be null");

    try {
      Instant recordBucket = hourBucket(ingestedRecord.kafkaTimestamp());
      Map<TopicPartitionKey, Long> boundaryOffsets = Map.of();
      Schema recordSchema = ingestedRecord.record().getSchema();
      boolean unseenSchema = !knownSchemas.contains(recordSchema);
      Schema mergedSchema = knownSourceSchema;
      boolean schemaChanged = false;

      if (unseenSchema) {
        mergedSchema = schemaEvolution.merge(knownSourceSchema, recordSchema);
        schemaChanged = knownSourceSchema != null && !mergedSchema.equals(knownSourceSchema);
      }

      if (writerOpen && shouldRotateWriter(recordBucket, schemaChanged)) {
        BatchPolicy.FlushReason reason =
            schemaChanged
                ? BatchPolicy.FlushReason.SCHEMA_CHANGE
                : BatchPolicy.FlushReason.TIME_BUCKET_BOUNDARY;

        log.info(
            "Batch flush triggered: reason={}, recordCount={},"
                + " accumulatedSizeBytes={}, firstRecordAt={}",
            reason,
            batchAccumulator.getRecordCount(),
            batchAccumulator.getAccumulatedSizeBytes(),
            batchAccumulator.getFirstRecordAt());

        boundaryOffsets = commitCurrentBatch(reason);
      }

      knownSourceSchema = mergedSchema;
      if (unseenSchema) {
        knownSchemas.add(recordSchema);
      }

      if (!writerOpen) {
        openWriter(ingestedRecord);
        currentBucketHour = recordBucket;
      }

      GenericRecord enrichedRecord = enrichWithKafkaMetadata(ingestedRecord);

      parquetFileWriter.write(currentWriter, enrichedRecord);
      metrics.recordWritten(instanceId);
      batchAccumulator.add(ingestedRecord);

      if (batchAccumulator.shouldFlush()) {
        Map<TopicPartitionKey, Long> offsets = commitCurrentBatch(batchAccumulator.flushReason());
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
    if (batchAccumulator.isEmpty()) {
      log.debug("Flush requested but accumulator is empty");
      return Map.of();
    }

    log.info("Executing lifecycle flush for {} records", batchAccumulator.getRecordCount());

    try {
      return commitCurrentBatch(BatchPolicy.FlushReason.LIFECYCLE);
    } catch (StorageException e) {
      handleWriteFailure();
      throw e;
    }
  }

  public Map<TopicPartitionKey, Long> flushRemaining() {
    if (batchAccumulator.isEmpty()) {
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
      return commitCurrentBatch(batchAccumulator.flushReason());
    } catch (StorageException e) {
      handleWriteFailure();
      throw e;
    }
  }

  public Optional<Map<TopicPartitionKey, Long>> recordSkippedOffset(
      String topic, int partition, long nextOffset) {
    return batchAccumulator.recordSkippedOffset(topic, partition, nextOffset);
  }

  private void openWriter(IngestedRecord firstRecord) {
    int primaryPartition = firstRecord.partition();

    String finalFileName =
        fileNameGenerator.generateFileName(primaryPartition, firstRecord.kafkaTimestamp());
    String tempFileName = fileNameGenerator.generateTempFileName(finalFileName);

    Path basePath = new Path(hdfsProperties.getBasePath());
    currentFinalPath = new Path(basePath, finalFileName);
    currentTempPath = new Path(basePath, tempFileName);
    currentStorageSchema = buildStorageSchema(knownSourceSchema);

    fileStorage.ensureDirectoryExists(currentFinalPath.getParent());
    currentWriter = parquetFileWriter.createWriter(currentTempPath, currentStorageSchema);
    writerOpen = true;

    log.debug("Opened streaming writer for partition {} at {}", primaryPartition, currentTempPath);
  }

  private Map<TopicPartitionKey, Long> commitCurrentBatch(BatchPolicy.FlushReason reason) {
    if (!writerOpen) {
      return Map.of();
    }

    long recordCount = batchAccumulator.getRecordCount();
    long sizeBytes = batchAccumulator.getAccumulatedSizeBytes();

    Timer.Sample sample = metrics.startRolloverTimer();

    log.info("Closing parquet writer for {}", currentTempPath);
    parquetFileWriter.closeWriter(currentWriter);
    writerOpen = false;

    fileStorage.commitAtomic(currentTempPath, currentFinalPath);
    log.info("Committed parquet file {}", currentFinalPath);

    metrics.stopRolloverTimer(instanceId, sample);
    metrics.rollover(instanceId, reason, recordCount, sizeBytes);

    Map<TopicPartitionKey, Long> offsets = batchAccumulator.getPendingOffsets();

    batchAccumulator.clear();

    currentWriter = null;
    currentTempPath = null;
    currentFinalPath = null;
    currentStorageSchema = null;

    return offsets;
  }

  private void handleWriteFailure() {
    metrics.writeFailure(instanceId);
    if (writerOpen) {
      try {
        parquetFileWriter.closeWriter(currentWriter);
      } catch (ParquetWriteException closeException) {
        log.warn("Failed closing writer during error handling", closeException);
      }
      writerOpen = false;
    }

    if (currentTempPath != null) {
      fileStorage.cleanupTempFile(currentTempPath);
    }

    batchAccumulator.clear();

    currentWriter = null;
    currentTempPath = null;
    currentFinalPath = null;
    currentStorageSchema = null;
  }

  private GenericRecord enrichWithKafkaMetadata(IngestedRecord ingestedRecord) {

    GenericRecord enriched = new GenericData.Record(currentStorageSchema);
    GenericRecord original = ingestedRecord.record();

    for (Schema.Field field : currentStorageSchema.getFields()) {
      Object value = switch (field.name()) {
        case "PROCESS_TIME" -> ingestedRecord.kafkaTimestamp();
        case "KAFKA_TOPIC" -> ingestedRecord.topic();
        case "KAFKA_PARTITION" -> ingestedRecord.partition();
        case "KAFKA_OFFSET" -> ingestedRecord.offset();
        default -> schemaEvolution.valueFor(original, field);
      };
      enriched.put(field.name(), value);
    }
    return enriched;
  }

  private boolean shouldRotateWriter(Instant recordBucket, boolean schemaChanged) {
    return !recordBucket.equals(currentBucketHour) || schemaChanged;
  }

  private Schema buildStorageSchema(Schema sourceSchema) {
    List<String> reservedFields =
        List.of("PROCESS_TIME", "KAFKA_TOPIC", "KAFKA_PARTITION", "KAFKA_OFFSET");
    for (String field : reservedFields) {
      if (sourceSchema.getField(field) != null) {
        throw new StorageException("Producer schema must not contain reserved field " + field);
      }
    }

    List<Schema.Field> fields = new ArrayList<>();
    sourceSchema.getFields().forEach(field -> fields.add(new Schema.Field(field, field.schema())));

    fields.add(
        storageField(
            "PROCESS_TIME", Schema.Type.LONG, "Kafka record timestamp in UTC epoch milliseconds."));
    fields.add(storageField("KAFKA_TOPIC", Schema.Type.STRING, "Kafka source topic."));
    fields.add(storageField("KAFKA_PARTITION", Schema.Type.INT, "Kafka source partition."));
    fields.add(storageField("KAFKA_OFFSET", Schema.Type.LONG, "Kafka source offset."));

    return Schema.createRecord(
        sourceSchema.getName(),
        sourceSchema.getDoc(),
        sourceSchema.getNamespace(),
        sourceSchema.isError(),
        fields);
  }

  private Schema.Field storageField(String name, Schema.Type type, String doc) {
    return new Schema.Field(
        name,
        Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(type)),
        doc,
        (Object) null);
  }
}
