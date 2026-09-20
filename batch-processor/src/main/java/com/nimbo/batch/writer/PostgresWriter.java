package com.nimbo.batch.writer;

import com.nimbo.batch.config.PostgresConfig;
import com.nimbo.batch.dao.ConnectionAggregateDao;
import com.nimbo.batch.dao.IpBandwidthAggregateDao;
import com.nimbo.batch.dao.ProtocolAggregateDao;
import com.nimbo.batch.dao.TrafficAggregateDao;
import com.nimbo.batch.metrics.BatchProcessorMetrics;
import com.nimbo.batch.metrics.BatchProcessorMetrics.Cube;
import com.nimbo.batch.metrics.BatchProcessorMetrics.Stage;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.util.LongAccumulator;
import org.springframework.stereotype.Component;

import java.io.Serial;
import java.io.Serializable;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static com.nimbo.batch.constants.ColumnConstants.*;

@Component
@Slf4j
public class PostgresWriter implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final TableRowMapper rowMapper;
    private final BatchExecutor batchExecutor;
    private final JdbiDaoFactory daoFactory;
    private final int batchSize;
    private final int maxRetryAttempts;
    private final long initialRetryDelayMs;
    private final long maxDelayMs;
    private final BatchProcessorMetrics metrics;

    public PostgresWriter(
            TableRowMapper rowMapper,
            BatchExecutor batchExecutor,
            JdbiDaoFactory daoFactory,
            PostgresConfig postgresConfig,
            BatchProcessorMetrics metrics) {

        this.rowMapper = rowMapper;
        this.batchExecutor = batchExecutor;
        this.daoFactory = daoFactory;
        this.batchSize = postgresConfig.getBatchSize();
        this.maxRetryAttempts = postgresConfig.getRetry().getMaxAttempts();
        this.initialRetryDelayMs = postgresConfig.getRetry().getInitialDelayMs();
        this.maxDelayMs = postgresConfig.getRetry().getMaxDelayMs();
        this.metrics = metrics;

        log.info(
                "PostgresWriter initialized - batchSize: {}, maxRetries: {}, retryDelayMs: {}, maxDelayMs: {}",
                batchSize, maxRetryAttempts, initialRetryDelayMs, maxDelayMs
        );
    }

    public long writeTrafficAggregates(Dataset<Row> dataset, UUID batchId) {
        return writeAggregates(
                dataset,
                batchId,
                "records_written_traffic_",
                TABLE_TRAFFIC_AGGREGATES,
                Cube.TRAFFIC,
                Stage.POSTGRES_TRAFFIC,
                JdbiDaoFactory::createTrafficDao,
                TrafficAggregateDao::upsertBatch,
                rowMapper::toTrafficAggregate
        );
    }

    public long writeConnectionAggregates(Dataset<Row> dataset, UUID batchId) {
        return writeAggregates(
                dataset,
                batchId,
                "records_written_connection_",
                TABLE_CONNECTION_AGGREGATES,
                Cube.CONNECTION,
                Stage.POSTGRES_CONNECTION,
                JdbiDaoFactory::createConnectionDao,
                ConnectionAggregateDao::upsertBatch,
                rowMapper::toConnectionAggregate
        );
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
        return writeAggregates(
                dataset,
                batchId,
                "records_written_ipbandwidth_",
                TABLE_IP_BANDWIDTH_AGGREGATES,
                Cube.TOP_IP,
                Stage.POSTGRES_TOP_IP,
                JdbiDaoFactory::createIpBandwidthDao,
                IpBandwidthAggregateDao::upsertBatch,
                rowMapper::toIpBandwidthAggregate
        );
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
            dataset.foreachPartition(partitionWriter::call);
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
