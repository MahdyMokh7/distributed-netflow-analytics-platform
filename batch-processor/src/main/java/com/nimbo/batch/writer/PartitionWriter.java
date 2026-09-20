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

@Slf4j
public class PartitionWriter<T, D> implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final String tableName;
    private final int batchSize;
    private final int maxRetryAttempts;
    private final long initialRetryDelayMs;
    private final long maxDelayMs;
    private final JdbiDaoFactory daoFactory;
    private final SerializableFunction<JdbiDaoFactory, D> daoCreator;
    private final SerializableBiFunction<D, List<T>, int[]> upsertFunction;
    private final RowMapper<T> rowMapper;
    private final UUID batchId;
    private final Timestamp ingestedAt;
    private final BatchExecutor batchExecutor;
    private final LongAccumulator accumulator;
    private final LongAccumulator retryAccumulator;

    public PartitionWriter(
            String tableName,
            int batchSize,
            int maxRetryAttempts,
            long initialRetryDelayMs,
            long maxDelayMs,
            JdbiDaoFactory daoFactory,
            SerializableFunction<JdbiDaoFactory, D> daoCreator,
            SerializableBiFunction<D, List<T>, int[]> upsertFunction,
            RowMapper<T> rowMapper,
            UUID batchId,
            Timestamp ingestedAt,
            BatchExecutor batchExecutor,
            LongAccumulator accumulator,
            LongAccumulator retryAccumulator) {

        this.tableName = tableName;
        this.batchSize = batchSize;
        this.maxRetryAttempts = maxRetryAttempts;
        this.initialRetryDelayMs = initialRetryDelayMs;
        this.maxDelayMs = maxDelayMs;
        this.daoFactory = daoFactory;
        this.daoCreator = daoCreator;
        this.upsertFunction = upsertFunction;
        this.rowMapper = rowMapper;
        this.batchId = batchId;
        this.ingestedAt = ingestedAt;
        this.batchExecutor = batchExecutor;
        this.accumulator = accumulator;
        this.retryAccumulator = retryAccumulator;
    }

    public void call(Iterator<Row> partition) {
        int partitionId = TaskContext.get() != null ? TaskContext.get().partitionId() : -1;
        int batchNumber = 0;
        List<T> batch = new ArrayList<>(batchSize);

        try {
            D dao = daoCreator.apply(daoFactory);
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
        } finally {
            daoFactory.close();
        }
    }

    private void flushBatch(List<T> batch, D dao) {
        int[] results = batchExecutor.executeWithRetry(
                () -> upsertFunction.apply(dao, new ArrayList<>(batch)),
                tableName,
                maxRetryAttempts,
                initialRetryDelayMs,
                maxDelayMs,
                () -> retryAccumulator.add(1L)
        );
        accumulator.add(batchExecutor.sumResults(results));
        batch.clear();
    }
}
