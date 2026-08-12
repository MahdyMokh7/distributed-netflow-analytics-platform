package com.nimbo.storage.service;

import com.nimbo.ingester.model.IngestedRecord;
import com.nimbo.storage.config.HdfsProperties;
import com.nimbo.storage.exception.StorageException;
import com.nimbo.storage.model.TopicPartitionKey;
import com.nimbo.storage.service.batching.BatchAccumulator;
import com.nimbo.storage.service.batching.RecordBatch;
import com.nimbo.storage.service.hdfs.FileStorage;
import com.nimbo.storage.service.naming.FileNameGenerator;
import com.nimbo.storage.service.parquet.ParquetFileWriter;
import org.apache.hadoop.fs.Path;
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
     batchAccumulator.add(record);

     if (batchAccumulator.shouldFlush()) {
        return Optional.of(flushInternal());
     }

     return Optional.empty();
     }

    public Map<TopicPartitionKey, Long> flush() {
        if (batchAccumulator.isEmpty()) {
            logger.info("Flush requested but accumulator is empty");
            return Map.of();
        }
        logger.info("Executing lifecycle flush for {} buffered records", batchAccumulator.getRecordCount());
        return flushInternal();
    }

    private Map<TopicPartitionKey, Long> flushInternal() {
        RecordBatch batch = batchAccumulator.peekBatch();
        int primaryPartition = batch.getRecords().get(0).partition();

        String finalFileName = fileNameGenerator.generateFileName(primaryPartition);
        String tempFileName = fileNameGenerator.generateTempFileName(finalFileName);

        Path basePath = new Path(hdfsProperties.getBasePath());
        Path finalPath = new Path(basePath, finalFileName);
        Path tempPath = new Path(basePath, tempFileName);

        try {
            fileStorage.ensureDirectoryExists(basePath);
            parquetFileWriter.writeToPath(batch, tempPath);
            fileStorage.commitAtomic(tempPath, finalPath);

            Map<TopicPartitionKey, Long> offsetsToCommit = batch.getOffsetsToCommit();

            batchAccumulator.clear();

            //logger.info("Successfully persisted batch of {} records to HDFS at {}", batch.size(), finalPath);
            return offsetsToCommit;

        } catch (Exception e) {
            fileStorage.cleanupTempFile(tempPath);
            logger.error("Failed to persist batch to HDFS. Buffer retained in memory for zero data loss.", e);
            throw new StorageException("Failed to write Parquet batch to HDFS", e);
        }
    }
}