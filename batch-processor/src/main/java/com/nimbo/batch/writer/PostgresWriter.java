package com.nimbo.batch.writer;

import com.nimbo.batch.config.PostgresConfig;
import com.nimbo.batch.dao.ConnectionAggregateDao;
import com.nimbo.batch.dao.IpBandwidthAggregateDao;
import com.nimbo.batch.dao.ProtocolAggregateDao;
import com.nimbo.batch.dao.TrafficAggregateDao;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.util.LongAccumulator;
import org.springframework.stereotype.Component;

import java.io.Serial;
import java.io.Serializable;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Supplier;

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

    public PostgresWriter(
            TableRowMapper rowMapper,
            BatchExecutor batchExecutor,
            JdbiDaoFactory daoFactory,
            PostgresConfig postgresConfig) {

        this.rowMapper = rowMapper;
        this.batchExecutor = batchExecutor;
        this.daoFactory = daoFactory;
        this.batchSize = postgresConfig.getBatchSize();
        this.maxRetryAttempts = postgresConfig.getRetry().getMaxAttempts();
        this.initialRetryDelayMs = postgresConfig.getRetry().getInitialDelayMs();
        this.maxDelayMs = postgresConfig.getRetry().getMaxDelayMs();

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
                daoFactory::createTrafficDao,
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
                daoFactory::createConnectionDao,
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
                daoFactory::createProtocolDao,
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
                daoFactory::createIpBandwidthDao,
                IpBandwidthAggregateDao::upsertBatch,
                rowMapper::toIpBandwidthAggregate
        );
    }

    private <T, D> long writeAggregates(
            Dataset<Row> dataset,
            UUID batchId,
            String accumulatorPrefix,
            String tableName,
            SerializableSupplier<D> daoSupplier,
            SerializableBiFunction<D, List<T>, int[]> upsertFunction,
            RowMapper<T> rowMapperFn) {

        LongAccumulator accumulator = dataset.sparkSession()
                .sparkContext()
                .longAccumulator(accumulatorPrefix + batchId);

        Timestamp ingestedAt = Timestamp.from(Instant.now());

        PartitionWriter<T, D> partitionWriter = new PartitionWriter<>(
                tableName,
                batchSize,
                maxRetryAttempts,
                initialRetryDelayMs,
                maxDelayMs,
                daoSupplier,
                upsertFunction,
                rowMapperFn,
                batchId,
                ingestedAt,
                batchExecutor,
                accumulator
        );

        // Pass the serializable writer to Spark
        dataset.foreachPartition(partitionWriter::call);

        long totalWritten = accumulator.value();
        log.info("Wrote {} records to {} with batch_id: {}", totalWritten, tableName, batchId);
        return totalWritten;
    }
}