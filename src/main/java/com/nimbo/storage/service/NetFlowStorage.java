package com.nimbo.storage.service;

import com.nimbo.ingester.model.IngestedRecord;
import com.nimbo.storage.config.HdfsProperties;
import com.nimbo.storage.exception.StorageException;
import com.nimbo.storage.model.TopicPartitionKey;
import com.nimbo.storage.service.batching.BatchAccumulator;
import com.nimbo.storage.service.hdfs.FileStorage;
import com.nimbo.storage.service.naming.FileNameGenerator;
import com.nimbo.storage.service.parquet.ParquetFileWriter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Service
public final class NetFlowStorage {

    private static final Logger logger = LoggerFactory.getLogger(NetFlowStorage.class);

    private final BatchAccumulator batchAccumulator;
    private final ParquetFileWriter parquetFileWriter;
    private final FileStorage fileStorage;
    private final FileNameGenerator fileNameGenerator;
    private final HdfsProperties hdfsProperties;

    private ParquetWriter<GenericRecord> currentWriter;
    private Path currentTempPath;
    private Path currentFinalPath;
    private Schema currentSchema;
    private boolean writerOpen = false;

    public NetFlowStorage(
            BatchAccumulator batchAccumulator,
            ParquetFileWriter parquetFileWriter,
            FileStorage fileStorage,
            FileNameGenerator fileNameGenerator,
            HdfsProperties hdfsProperties
    ) {
        this.batchAccumulator = Objects.requireNonNull(batchAccumulator, "batchAccumulator must not be null");
        this.parquetFileWriter = Objects.requireNonNull(parquetFileWriter, "parquetFileWriter must not be null");
        this.fileStorage = Objects.requireNonNull(fileStorage, "fileStorage must not be null");
        this.fileNameGenerator = Objects.requireNonNull(fileNameGenerator, "fileNameGenerator must not be null");
        this.hdfsProperties = Objects.requireNonNull(hdfsProperties, "hdfsProperties must not be null");

        logger.info("NetFlowStorage initialized with HDFS target path: {}", hdfsProperties.getBasePath());
    }

    public Optional<Map<TopicPartitionKey, Long>> accept(IngestedRecord record) {
        Objects.requireNonNull(record, "record must not be null");

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
            logger.info("Flush requested but accumulator is empty");
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