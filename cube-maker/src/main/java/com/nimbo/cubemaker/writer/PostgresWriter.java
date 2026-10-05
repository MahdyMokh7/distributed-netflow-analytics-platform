package com.nimbo.cubemaker.writer;

import com.nimbo.cubemaker.config.PostgresConfig;
import com.nimbo.cubemaker.dao.ProtocolAggregateDao;
import com.nimbo.cubemaker.metrics.CubeMakerMetrics;
import com.nimbo.cubemaker.metrics.CubeMakerMetrics.Cube;
import com.nimbo.cubemaker.metrics.CubeMakerMetrics.Stage;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.util.LongAccumulator;
import org.jdbi.v3.core.Jdbi;
import org.springframework.stereotype.Component;

import java.io.Serial;
import java.io.Serializable;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static com.nimbo.cubemaker.constants.ColumnConstants.*;

@Component
@Slf4j
public class PostgresWriter implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final TableRowMapper rowMapper;
    private final BatchExecutor batchExecutor;
    private final JdbiDaoFactory daoFactory;
    private final int batchSize;
    private final int writerPartitions;
    private final int maxRetryAttempts;
    private final long initialRetryDelayMs;
    private final long maxDelayMs;
    private final CubeMakerMetrics metrics;
    private final transient Jdbi driverJdbi;

    public PostgresWriter(
            TableRowMapper rowMapper,
            BatchExecutor batchExecutor,
            JdbiDaoFactory daoFactory,
            PostgresConfig postgresConfig,
            CubeMakerMetrics metrics,
            Jdbi driverJdbi) {

        this.rowMapper = rowMapper;
        this.batchExecutor = batchExecutor;
        this.daoFactory = daoFactory;
        this.batchSize = postgresConfig.getBatchSize();
        this.writerPartitions = postgresConfig.getWriterPartitions();
        this.maxRetryAttempts = postgresConfig.getRetry().getMaxAttempts();
        this.initialRetryDelayMs = postgresConfig.getRetry().getInitialDelayMs();
        this.maxDelayMs = postgresConfig.getRetry().getMaxDelayMs();
        this.metrics = metrics;
        this.driverJdbi = driverJdbi;

        log.info(
                "PostgresWriter initialized - batchSize: {}, writerPartitions: {}, maxRetries: {}, retryDelayMs: {}, maxDelayMs: {}",
                batchSize, writerPartitions, maxRetryAttempts, initialRetryDelayMs, maxDelayMs
        );
    }

    public long writeTrafficAggregates(Dataset<Row> dataset, UUID batchId) {
        return writeStagedAggregates(dataset, batchId, TABLE_TRAFFIC_AGGREGATES,
                Cube.TRAFFIC, Stage.POSTGRES_TRAFFIC, StagingTable.TRAFFIC);
    }

    public long writeConnectionAggregates(Dataset<Row> dataset, UUID batchId) {
        return writeStagedAggregates(dataset, batchId, TABLE_CONNECTION_AGGREGATES,
                Cube.CONNECTION, Stage.POSTGRES_CONNECTION,
                StagingTable.CONNECTION);
    }

    public long writeProtocolAggregates(Dataset<Row> dataset, UUID batchId) {
        return writeAggregates(
                dataset,
                batchId,
                "records_written_protocol_",
                TABLE_PROTOCOL_USAGE_AGGREGATES,
                Cube.PROTOCOL,
                Stage.POSTGRES_PROTOCOL,
                JdbiDaoFactory::createProtocolDao,
                ProtocolAggregateDao::upsertBatch,
                rowMapper::toProtocolAggregate
        );
    }

    public long writeIpBandwidthAggregates(Dataset<Row> dataset, UUID batchId) {
        return writeStagedAggregates(dataset, batchId, TABLE_IP_BANDWIDTH_AGGREGATES,
                Cube.TOP_IP, Stage.POSTGRES_TOP_IP,
                StagingTable.BANDWIDTH);
    }

    private long writeStagedAggregates(
            Dataset<Row> dataset,
            UUID batchId,
            String tableName,
            Cube cube,
            Stage writeStage,
            StagingTable stagingTable) {

        Timestamp ingestedAt = Timestamp.from(Instant.now());

        driverJdbi.useHandle(handle -> handle.createUpdate(
                        "DELETE FROM " + stagingTable.stagingTable() + " WHERE batch_id = :batchId")
                .bind("batchId", batchId)
                .execute());

        StagingPartitionWriter stagingWriter = new StagingPartitionWriter(
                stagingTable, daoFactory, batchId, ingestedAt);

        Timer.Sample writeTimer = metrics.startTimer();
        try {
            dataset.repartition(writerPartitions).foreachPartition(stagingWriter::call);
            long stagedRows = driverJdbi.withHandle(handle -> handle.createQuery(
                            "SELECT COUNT(*) FROM " + stagingTable.stagingTable()
                                    + " WHERE batch_id = :batchId")
                    .bind("batchId", batchId)
                    .mapTo(Long.class)
                    .one());
            int mergedRows = driverJdbi.inTransaction(handle -> {
                int merged = handle.createUpdate(stagingTable.mergeSql())
                        .bind("batchId", batchId)
                        .execute();
                handle.createUpdate("DELETE FROM " + stagingTable.stagingTable()
                                + " WHERE batch_id = :batchId")
                        .bind("batchId", batchId)
                        .execute();
                return merged;
            });
            if (mergedRows != stagedRows) {
                throw new IllegalStateException("Staged row count " + stagedRows
                        + " does not match merged row count " + mergedRows + " for " + tableName);
            }
        } catch (RuntimeException | Error exception) {
            metrics.stageFailed(writeStage);
            throw exception;
        } finally {
            metrics.stageFinished(writeTimer, writeStage);
            metrics.postgresRetries(cube, 0L);
        }

        long totalWritten = mergedRowsForBatch(tableName, batchId);
        metrics.outputRows(cube, totalWritten);
        log.info("Staged and merged {} records into {} with batch_id: {}",
                totalWritten, tableName, batchId);
        return totalWritten;
    }

    private long mergedRowsForBatch(String tableName, UUID batchId) {
        return driverJdbi.withHandle(handle -> handle.createQuery(
                        "SELECT COUNT(*) FROM " + tableName + " WHERE batch_id = :batchId")
                .bind("batchId", batchId)
                .mapTo(Long.class)
                .one());
    }

    private <T, D> long writeAggregates(
            Dataset<Row> dataset,
            UUID batchId,
            String accumulatorPrefix,
            String tableName,
            Cube cube,
            Stage writeStage,
            SerializableFunction<JdbiDaoFactory, D> daoCreator,
            SerializableBiFunction<D, List<T>, int[]> upsertFunction,
            RowMapper<T> rowMapperFn) {

        LongAccumulator accumulator = dataset.sparkSession()
                .sparkContext()
                .longAccumulator(accumulatorPrefix + batchId);
        LongAccumulator retryAccumulator = dataset.sparkSession()
                .sparkContext()
                .longAccumulator("postgres_retries_" + cube.tagValue() + "_" + batchId);

        Timestamp ingestedAt = Timestamp.from(Instant.now());

        PartitionWriter<T, D> partitionWriter = new PartitionWriter<>(
                tableName,
                batchSize,
                maxRetryAttempts,
                initialRetryDelayMs,
                maxDelayMs,
                daoFactory,
                daoCreator,
                upsertFunction,
                rowMapperFn,
                batchId,
                ingestedAt,
                batchExecutor,
                accumulator,
                retryAccumulator
        );

        Timer.Sample writeTimer = metrics.startTimer();
        try {
            dataset.repartition(writerPartitions).foreachPartition(partitionWriter::call);
        } catch (RuntimeException | Error e) {
            metrics.stageFailed(writeStage);
            throw e;
        } finally {
            metrics.stageFinished(writeTimer, writeStage);
            metrics.postgresRetries(cube, retryAccumulator.value());
        }

        long totalWritten = accumulator.value();
        metrics.outputRows(cube, totalWritten);
        log.info("Wrote {} records to {} with batch_id: {}", totalWritten, tableName, batchId);
        return totalWritten;
    }
}
