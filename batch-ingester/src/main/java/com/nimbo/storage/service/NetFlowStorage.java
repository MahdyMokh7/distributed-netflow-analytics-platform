package com.nimbo.storage.service;

import com.nimbo.ingester.model.IngestedRecord;
import com.nimbo.metrics.BatchIngesterMetrics;
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
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Slf4j
public final class NetFlowStorage {

    private final ParquetFileWriter parquetFileWriter;
    private final FileStorage fileStorage;
    private final FileNameGenerator fileNameGenerator;
    private final HdfsProperties hdfsProperties;
    private final Schema storageSchema;

    private final BatchAccumulator batchAccumulator;

    private ParquetWriter<GenericRecord> currentWriter;
    private Path currentTempPath;
    private Path currentFinalPath;
    private boolean writerOpen = false;

    private final BatchIngesterMetrics metrics;
    private final int instanceId;

    public NetFlowStorage(
            BatchPolicy batchPolicy,
            ParquetFileWriter parquetFileWriter,
            FileStorage fileStorage,
            FileNameGenerator fileNameGenerator,
            HdfsProperties hdfsProperties,
            Schema storageSchema,
            BatchIngesterMetrics metrics,
            int instanceId
    ) {
        this.parquetFileWriter = Objects.requireNonNull(parquetFileWriter);
        this.fileStorage = Objects.requireNonNull(fileStorage);
        this.fileNameGenerator = Objects.requireNonNull(fileNameGenerator);
        this.hdfsProperties = Objects.requireNonNull(hdfsProperties);
        this.storageSchema = Objects.requireNonNull(storageSchema);
        this.batchAccumulator = new BatchAccumulator(batchPolicy);
        this.metrics = metrics;
        this.instanceId = instanceId;

        metrics.registerGauge("ingester_open_batch_record_count", instanceId,
                batchAccumulator, BatchAccumulator::getRecordCount);
        metrics.registerGauge("ingester_open_batch_age_seconds", instanceId,
                this, NetFlowStorage::currentBatchAgeSeconds);

        log.info("NetFlowStorage initialized (streaming writer) with HDFS target path: {}", hdfsProperties.getBasePath());
    }

    private double currentBatchAgeSeconds() {
        if (!writerOpen) return 0;
        Instant firstRecordAt = batchAccumulator.getFirstRecordAt();
        return firstRecordAt == null ? 0 : Duration.between(firstRecordAt, Instant.now()).getSeconds();
    }

    public Optional<Map<TopicPartitionKey, Long>> accept(IngestedRecord ingestedRecord) {
        Objects.requireNonNull(ingestedRecord, "ingestedRecord must not be null");

        try {
            if (!writerOpen) {
                openWriter(ingestedRecord);
            }

            GenericRecord enrichedRecord = enrichWithKafkaTime(
                    ingestedRecord.record(),
                    ingestedRecord.kafkaTimestamp()
            );
            
            parquetFileWriter.write(currentWriter, enrichedRecord);
            metrics.recordWritten(instanceId);
            batchAccumulator.add(ingestedRecord);

            if (batchAccumulator.shouldFlush()) {
                Map<TopicPartitionKey, Long> offsets = commitCurrentBatch();
                return Optional.of(offsets);
            }

            return Optional.empty();

        } catch (StorageException e) {
            handleWriteFailure();

            log.error(
                    "Failed writing record: topic={}, partition={}, offset={}",
                    ingestedRecord.topic(),
                    ingestedRecord.partition(),
                    ingestedRecord.offset(),
                    e
            );

            throw e;
        }
    }

    public Map<TopicPartitionKey, Long> flush() {
        if (batchAccumulator.isEmpty()) {
            log.debug("Flush requested but accumulator is empty");
            return Map.of();
        }

        log.info("Executing lifecycle flush for {} records", batchAccumulator.getRecordCount());

        try {
            return commitCurrentBatch();
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
                batchAccumulator.getAccumulatedSizeBytes()
        );

        try {
            return commitCurrentBatch();
        } catch (StorageException e) {
            handleWriteFailure();
            throw e;
        }
    }

    public Optional<Map<TopicPartitionKey, Long>> recordSkippedOffset(String topic, int partition, long nextOffset) {
        return batchAccumulator.recordSkippedOffset(topic, partition, nextOffset);
    }

    private void openWriter(IngestedRecord firstRecord)  {
        int primaryPartition = firstRecord.partition();

        String finalFileName = fileNameGenerator.generateFileName(primaryPartition, firstRecord.kafkaTimestamp());
        String tempFileName = fileNameGenerator.generateTempFileName(finalFileName);

        Path basePath = new Path(hdfsProperties.getBasePath());
        currentFinalPath = new Path(basePath, finalFileName);
        currentTempPath = new Path(basePath, tempFileName);

        fileStorage.ensureDirectoryExists(currentFinalPath.getParent());
        currentWriter = parquetFileWriter.createWriter(currentTempPath, storageSchema);
        writerOpen = true;

        log.debug("Opened streaming writer for partition {} at {}", primaryPartition, currentTempPath);
    }

    private Map<TopicPartitionKey, Long> commitCurrentBatch() {
        if (!writerOpen) {
            return Map.of();
        }

        long recordCount = batchAccumulator.getRecordCount();
        long sizeBytes = batchAccumulator.getAccumulatedSizeBytes();
        BatchPolicy.FlushReason reason = batchAccumulator.flushReason();

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
    }

    private GenericRecord enrichWithKafkaTime(GenericRecord original, long kafkaTimestamp) {

        GenericRecord enriched = new GenericData.Record(storageSchema);
        long kafkaTimestampMicros = kafkaTimestamp * 1000;

        for (Schema.Field field : storageSchema.getFields()) {
            String fieldName = field.name();

            if ("PROCESS_TIME".equals(fieldName)) {
                enriched.put("PROCESS_TIME", kafkaTimestampMicros);
            } else {
                enriched.put(fieldName, original.get(fieldName));
            }
        }
        return enriched;
    }
}
