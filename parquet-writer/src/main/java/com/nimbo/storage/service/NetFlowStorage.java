package com.nimbo.storage.service;

import com.nimbo.common.netflow.NetflowMetadata;
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
  private final HourWindowPolicy hourWindowPolicy;
  private final StorageSchemaEvolution schemaEvolution = new StorageSchemaEvolution();

  private final BatchAccumulator batchAccumulator;
  private final Map<Instant, WriterState> writers = new HashMap<>();

  private final ParquetWriterMetrics metrics;
  private final int instanceId;
  private Schema knownSourceSchema;
  private final Set<Schema> knownSchemas = new HashSet<>();

  public NetFlowStorage(
      BatchPolicy batchPolicy,
      ParquetFileWriter parquetFileWriter,
      FileStorage fileStorage,
      FileNameGenerator fileNameGenerator,
      HdfsProperties hdfsProperties,
      HourWindowPolicy hourWindowPolicy,
      ParquetWriterMetrics metrics,
      int instanceId,
      List<Schema> registeredSchemas) {
    this.parquetFileWriter = Objects.requireNonNull(parquetFileWriter);
    this.fileStorage = Objects.requireNonNull(fileStorage);
    this.fileNameGenerator = Objects.requireNonNull(fileNameGenerator);
    this.hdfsProperties = Objects.requireNonNull(hdfsProperties);
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

      WriterState writerState = writers.get(recordBucket);
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

      GenericRecord enrichedRecord =
          enrichWithKafkaMetadata(ingestedRecord, writerState.storageSchema);

      parquetFileWriter.write(writerState.writer, enrichedRecord);
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

  private WriterState openWriter(IngestedRecord firstRecord) {
    int primaryPartition = firstRecord.partition();

    String finalFileName =
        fileNameGenerator.generateFileName(primaryPartition, firstRecord.kafkaTimestamp());
    String tempFileName = fileNameGenerator.generateTempFileName(finalFileName);

    Path basePath = new Path(hdfsProperties.getBasePath());
    Path finalPath = new Path(basePath, finalFileName);
    Path tempPath = new Path(basePath, tempFileName);
    Schema storageSchema = buildStorageSchema(knownSourceSchema);

    try {
      fileStorage.ensureDirectoryExists(finalPath.getParent());
      ParquetWriter<GenericRecord> writer = parquetFileWriter.createWriter(tempPath, storageSchema);
      log.debug("Opened streaming writer for partition {} at {}", primaryPartition, tempPath);
      return new WriterState(writer, tempPath, finalPath, storageSchema);
    } catch (StorageException exception) {
      fileStorage.cleanupTempFile(tempPath);
      throw exception;
    }
  }

  private Map<TopicPartitionKey, Long> commitOpenWriters(BatchPolicy.FlushReason reason) {
    if (writers.isEmpty()) {
      return Map.of();
    }

    long recordCount = batchAccumulator.getRecordCount();
    long sizeBytes = batchAccumulator.getAccumulatedSizeBytes();

    Timer.Sample sample = metrics.startRolloverTimer();

    for (WriterState writerState : writers.values()) {
      closeWriter(writerState);
    }
    for (WriterState writerState : writers.values()) {
      fileStorage.commitAtomic(writerState.tempPath, writerState.finalPath);
      metrics.parquetFileCommitted(instanceId);
      log.info("Committed parquet file {}", writerState.finalPath);
    }

    metrics.stopRolloverTimer(instanceId, sample);
    metrics.rollover(instanceId, reason, recordCount, sizeBytes);

    Map<TopicPartitionKey, Long> offsets = batchAccumulator.getPendingOffsets();

    batchAccumulator.clear();
    writers.clear();

    return offsets;
  }

  private void closeWriter(WriterState writerState) {
    if (writerState.closed) {
      return;
    }
    log.info("Closing parquet writer for {}", writerState.tempPath);
    parquetFileWriter.closeWriter(writerState.writer);
    writerState.closed = true;
  }

  private void handleWriteFailure() {
    metrics.writeFailure(instanceId);
    for (WriterState writerState : writers.values()) {
      try {
        closeWriter(writerState);
      } catch (ParquetWriteException closeException) {
        log.warn("Failed closing writer during error handling", closeException);
      }
      fileStorage.cleanupTempFile(writerState.tempPath);
    }

    batchAccumulator.clear();
    writers.clear();
  }

  private GenericRecord enrichWithKafkaMetadata(
      IngestedRecord ingestedRecord, Schema storageSchema) {

    GenericRecord enriched = new GenericData.Record(storageSchema);
    GenericRecord original = ingestedRecord.record();

    for (Schema.Field field : storageSchema.getFields()) {
      Object value = switch (field.name()) {
        case NetflowMetadata.PROCESS_TIME -> ingestedRecord.kafkaTimestamp();
        case NetflowMetadata.KAFKA_TOPIC -> ingestedRecord.topic();
        case NetflowMetadata.KAFKA_PARTITION -> ingestedRecord.partition();
        case NetflowMetadata.KAFKA_OFFSET -> ingestedRecord.offset();
        default -> schemaEvolution.valueFor(original, field);
      };
      enriched.put(field.name(), value);
    }
    return enriched;
  }

  private Schema buildStorageSchema(Schema sourceSchema) {
    for (String field : NetflowMetadata.RESERVED_FIELDS) {
      if (sourceSchema.getField(field) != null) {
        throw new StorageException("Producer schema must not contain reserved field " + field);
      }
    }

    List<Schema.Field> fields = new ArrayList<>();
    sourceSchema.getFields().forEach(field -> fields.add(new Schema.Field(field, field.schema())));

    fields.add(
        storageField(
            NetflowMetadata.PROCESS_TIME,
            Schema.Type.LONG,
            NetflowMetadata.PROCESS_TIME_DOC));
    fields.add(
        storageField(
            NetflowMetadata.KAFKA_TOPIC,
            Schema.Type.STRING,
            NetflowMetadata.KAFKA_TOPIC_DOC));
    fields.add(
        storageField(
            NetflowMetadata.KAFKA_PARTITION,
            Schema.Type.INT,
            NetflowMetadata.KAFKA_PARTITION_DOC));
    fields.add(
        storageField(
            NetflowMetadata.KAFKA_OFFSET,
            Schema.Type.LONG,
            NetflowMetadata.KAFKA_OFFSET_DOC));

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

  private static final class WriterState {

    private final ParquetWriter<GenericRecord> writer;
    private final Path tempPath;
    private final Path finalPath;
    private final Schema storageSchema;
    private boolean closed;

    private WriterState(
        ParquetWriter<GenericRecord> writer,
        Path tempPath,
        Path finalPath,
        Schema storageSchema) {
      this.writer = writer;
      this.tempPath = tempPath;
      this.finalPath = finalPath;
      this.storageSchema = storageSchema;
    }
  }
}
