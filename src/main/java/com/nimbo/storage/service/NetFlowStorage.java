package com.nimbo.storage.service;

import com.nimbo.ingester.model.IngestedRecord;
import com.nimbo.storage.config.HdfsProperties;
import com.nimbo.storage.exception.StorageException;
import com.nimbo.storage.model.TopicPartitionKey;
import com.nimbo.storage.service.batching.BatchAccumulator;
import com.nimbo.storage.service.batching.BatchPolicy;
import com.nimbo.storage.service.hdfs.FileStorage;
import com.nimbo.storage.service.naming.FileNameGenerator;
import com.nimbo.storage.service.parquet.ParquetFileWriter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public final class NetFlowStorage {

    private static final Logger logger = LoggerFactory.getLogger(NetFlowStorage.class);

    private final BatchPolicy batchPolicy;
    private final ParquetFileWriter parquetFileWriter;
    private final FileStorage fileStorage;
    private final FileNameGenerator fileNameGenerator;
    private final HdfsProperties hdfsProperties;

    private final BatchAccumulator batchAccumulator;

    private ParquetWriter<GenericRecord> currentWriter;
    private Path currentTempPath;
    private Path currentFinalPath;
    private Schema currentSchema;
    private boolean writerOpen = false;

    public NetFlowStorage(
            BatchPolicy batchPolicy,
            ParquetFileWriter parquetFileWriter,
            FileStorage fileStorage,
            FileNameGenerator fileNameGenerator,
            HdfsProperties hdfsProperties
    ) {
        this.batchPolicy = Objects.requireNonNull(batchPolicy);
        this.parquetFileWriter = Objects.requireNonNull(parquetFileWriter);
        this.fileStorage = Objects.requireNonNull(fileStorage);
        this.fileNameGenerator = Objects.requireNonNull(fileNameGenerator);
        this.hdfsProperties = Objects.requireNonNull(hdfsProperties);

        this.batchAccumulator = new BatchAccumulator(batchPolicy);

        logger.info("NetFlowStorage initialized (streaming writer) with HDFS target path: {}", hdfsProperties.getBasePath());
    }

    public Optional<Map<TopicPartitionKey, Long>> accept(IngestedRecord record) {
        Objects.requireNonNull(record);

        try {
            if (!writerOpen) {
                openWriter(record);
            }

            parquetFileWriter.write(currentWriter, record.record());

            batchAccumulator.add(record);

            if (batchAccumulator.shouldFlush()) {
                Map<TopicPartitionKey, Long> offsets = commitCurrentBatch();
                return Optional.of(offsets);
            }

            return Optional.empty();

        } catch (Exception e) {
            handleWriteFailure(e);
            throw new StorageException("Failed to process record in streaming Parquet writer", e);
        }
    }

    public Map<TopicPartitionKey, Long> flush() {
        if (batchAccumulator.isEmpty()) {
            logger.debug("Flush requested but accumulator is empty");
            return Map.of();
        }

        logger.info("Executing lifecycle flush for {} records", batchAccumulator.getRecordCount());

        try {
            return commitCurrentBatch();
        } catch (Exception e) {
            handleWriteFailure(e);
            throw new StorageException("Failed to flush Parquet batch to HDFS", e);
        }
    }

    public Map<TopicPartitionKey, Long> flushIfNeeded() {
        if (batchAccumulator.isEmpty()) {
            return Map.of();
        }

        if (!batchAccumulator.shouldFlush()) {
            return Map.of();
        }

        logger.info(
                "Flushed remainings, Partition is Empty now...",
                batchAccumulator.getRecordCount(),
                batchAccumulator.getAccumulatedSizeBytes()
        );

        try {
            return commitCurrentBatch();
        } catch (Exception e) {
            handleWriteFailure(e);
            throw new StorageException("Failed to flush Parquet batch to HDFS", e);
        }
    }

    private void openWriter(IngestedRecord firstRecord) throws Exception {
        int primaryPartition = firstRecord.partition();

        String finalFileName = fileNameGenerator.generateFileName(primaryPartition);
        String tempFileName = fileNameGenerator.generateTempFileName(finalFileName);

        Path basePath = new Path(hdfsProperties.getBasePath());
        currentFinalPath = new Path(basePath, finalFileName);
        currentTempPath = new Path(basePath, tempFileName);

        fileStorage.ensureDirectoryExists(basePath);
        currentSchema = firstRecord.record().getSchema();
        currentWriter = parquetFileWriter.createWriter(currentTempPath, currentSchema);
        writerOpen = true;

        logger.debug("Opened streaming writer for partition {} at {}", primaryPartition, currentTempPath);
    }

    private Map<TopicPartitionKey, Long> commitCurrentBatch() throws Exception {
        if (!writerOpen) {
            return Map.of();
        }

        parquetFileWriter.closeWriter(currentWriter);
        writerOpen = false;

        fileStorage.commitAtomic(currentTempPath, currentFinalPath);

        Map<TopicPartitionKey, Long> offsets = batchAccumulator.getPendingOffsets();

        batchAccumulator.clear();

        currentWriter = null;
        currentTempPath = null;
        currentFinalPath = null;
        currentSchema = null;

        return offsets;
    }

    private void handleWriteFailure(Exception e) {
        if (writerOpen) {
            try {
                parquetFileWriter.closeWriter(currentWriter);
            } catch (Exception closeException) {
                logger.warn("Failed closing writer during error handling", closeException);
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
        currentSchema = null;
    }
}