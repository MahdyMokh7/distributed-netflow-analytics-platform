package com.nimbo.batch.writer;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.Row;
import org.apache.spark.util.LongAccumulator;

import java.io.Serial;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Supplier;

@Slf4j
public class PartitionWriter<T, D> implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final String tableName;
    private final int batchSize;
    private final int maxRetryAttempts;
    private final long initialRetryDelayMs;
    private final long maxDelayMs;
    private final SerializableSupplier<D> daoSupplier;
    private final SerializableBiFunction<D, List<T>, int[]> upsertFunction;
    private final RowMapper<T> rowMapper;
    private final UUID batchId;
    private final Timestamp ingestedAt;
    private final BatchExecutor batchExecutor;
    private final LongAccumulator accumulator;

    public PartitionWriter(
            String tableName,
            int batchSize,
            int maxRetryAttempts,
            long initialRetryDelayMs,
            long maxDelayMs,
            SerializableSupplier<D> daoSupplier,
            SerializableBiFunction<D, List<T>, int[]> upsertFunction,
            RowMapper<T> rowMapper,
            UUID batchId,
            Timestamp ingestedAt,
            BatchExecutor batchExecutor,
            LongAccumulator accumulator) {

        this.tableName = tableName;
        this.batchSize = batchSize;
        this.maxRetryAttempts = maxRetryAttempts;
        this.initialRetryDelayMs = initialRetryDelayMs;
        this.maxDelayMs = maxDelayMs;
        this.daoSupplier = daoSupplier;
        this.upsertFunction = upsertFunction;
        this.rowMapper = rowMapper;
        this.batchId = batchId;
        this.ingestedAt = ingestedAt;
        this.batchExecutor = batchExecutor;
        this.accumulator = accumulator;
    }

    public void call(Iterator<Row> partition) {
        int partitionId = TaskContext.get() != null ? TaskContext.get().partitionId() : -1;
        int batchNumber = 0;
        List<T> batch = new ArrayList<>(batchSize);

        // Create DAO once per partition
        D dao = daoSupplier.get();

        while (partition.hasNext()) {
            Row row = partition.next();
            batch.add(rowMapper.map(row, batchId, ingestedAt));

            if (batch.size() >= batchSize) {
                batchNumber++;
                log.info("Partition {} - Writing batch #{} ({} rows) to {}",
                        partitionId, batchNumber, batch.size(), tableName);
                flushBatch(batch, dao);
            }
        }

        if (!batch.isEmpty()) {
            batchNumber++;
            log.info("Partition {} - Writing final batch #{} ({} rows) to {}",
                    partitionId, batchNumber, batch.size(), tableName);
            flushBatch(batch, dao);
        }
    }

    private void flushBatch(List<T> batch, D dao) {
        int[] results = batchExecutor.executeWithRetry(
                () -> upsertFunction.apply(dao, new ArrayList<>(batch)),
                tableName,
                maxRetryAttempts,
                initialRetryDelayMs,
                maxDelayMs
        );
        accumulator.add(batchExecutor.sumResults(results));
        batch.clear();
    }
}