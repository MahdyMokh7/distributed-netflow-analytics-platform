package com.nimbo.storage;

import com.nimbo.config.HdfsProperties;
import com.nimbo.metrics.ParquetWriterMetrics;
import com.nimbo.model.IngestedRecord;
import com.nimbo.model.TopicPartitionKey;
import com.nimbo.storage.batching.BatchAccumulator;
import com.nimbo.storage.batching.BatchPolicy;
import com.nimbo.storage.exception.ParquetWriteException;
import com.nimbo.storage.exception.StorageException;
import com.nimbo.storage.hdfs.FileStorage;
import com.nimbo.storage.naming.FileNameGenerator;
import com.nimbo.storage.parquet.ParquetFileWriter;
import io.micrometer.core.instrument.Timer;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;

@Slf4j(topic = "com.nimbo.storage.NetFlowStorage")
final class ParquetFileLifecycle {

  private final ParquetFileWriter parquetFileWriter;
  private final FileStorage fileStorage;
  private final FileNameGenerator fileNameGenerator;
  private final HdfsProperties hdfsProperties;

  ParquetFileLifecycle(
      ParquetFileWriter parquetFileWriter,
      FileStorage fileStorage,
      FileNameGenerator fileNameGenerator,
      HdfsProperties hdfsProperties) {
    this.parquetFileWriter = Objects.requireNonNull(parquetFileWriter);
    this.fileStorage = Objects.requireNonNull(fileStorage);
    this.fileNameGenerator = Objects.requireNonNull(fileNameGenerator);
    this.hdfsProperties = Objects.requireNonNull(hdfsProperties);
  }

  WriterState openWriter(IngestedRecord firstRecord, Schema knownSourceSchema, ParquetRecordMapper recordMapper) {
    int primaryPartition = firstRecord.partition();

    String finalFileName =
        fileNameGenerator.generateFileName(primaryPartition, firstRecord.kafkaTimestamp());
    String tempFileName = fileNameGenerator.generateTempFileName(finalFileName);

    Path basePath = new Path(hdfsProperties.getBasePath());
    Path finalPath = new Path(basePath, finalFileName);
    Path tempPath = new Path(basePath, tempFileName);
    Schema storageSchema = recordMapper.buildStorageSchema(knownSourceSchema);

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

  Map<TopicPartitionKey, Long> commitOpenWriters(
      Map<Instant, WriterState> writers,
      BatchAccumulator batchAccumulator,
      BatchPolicy.FlushReason reason,
      ParquetWriterMetrics metrics,
      int instanceId) {

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

  void handleWriteFailure(
      Map<Instant, WriterState> writers,
      BatchAccumulator batchAccumulator,
      ParquetWriterMetrics metrics,
      int instanceId) {

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

  void write(WriterState writerState, GenericRecord record) {
    parquetFileWriter.write(writerState.writer, record);
  }

  private void closeWriter(WriterState writerState) {
    if (writerState.closed) {
      return;
    }
    log.info("Closing parquet writer for {}", writerState.tempPath);
    parquetFileWriter.closeWriter(writerState.writer);
    writerState.closed = true;
  }

  static final class WriterState {

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

    Schema storageSchema() {
      return storageSchema;
    }
  }
}
