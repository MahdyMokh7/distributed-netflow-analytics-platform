package com.nimbo.batch.writer;

import com.nimbo.batch.config.PostgresConfig;
import com.nimbo.batch.dao.ConnectionAggregateDao;
import com.nimbo.batch.dao.IpBandwidthAggregateDao;
import com.nimbo.batch.dao.ProtocolAggregateDao;
import com.nimbo.batch.dao.TrafficAggregateDao;
import com.nimbo.batch.model.ConnectionAggregate;
import com.nimbo.batch.model.ProtocolAggregate;
import com.nimbo.batch.model.TopIpAggregate;
import com.nimbo.batch.model.TrafficAggregate;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

@Component
@Slf4j
public class PostgresWriter {

    private final TrafficAggregateDao trafficDao;
    private final ConnectionAggregateDao connectionDao;
    private final ProtocolAggregateDao protocolDao;
    private final IpBandwidthAggregateDao ipBandwidthDao;
    private final TableRowMapper rowMapper;
    private final BatchExecutor batchExecutor;
    private final int batchSize;
    private final int maxRetryAttempts;
    private final long initialRetryDelayMs;

    public PostgresWriter(
            TrafficAggregateDao trafficDao,
            ConnectionAggregateDao connectionDao,
            ProtocolAggregateDao protocolDao,
            IpBandwidthAggregateDao ipBandwidthDao,
            TableRowMapper rowMapper,
            BatchExecutor batchExecutor,
            PostgresConfig postgresConfig) {

        this.trafficDao = trafficDao;
        this.connectionDao = connectionDao;
        this.protocolDao = protocolDao;
        this.ipBandwidthDao = ipBandwidthDao;
        this.rowMapper = rowMapper;
        this.batchExecutor = batchExecutor;
        this.batchSize = postgresConfig.getBatchSize();
        this.maxRetryAttempts = postgresConfig.getRetry().getMaxAttempts();
        this.initialRetryDelayMs = postgresConfig.getRetry().getInitialDelayMs();

        log.info(
                "PostgresWriter initialized - batchSize: {}, maxRetries: {}, retryDelayMs: {}",
                batchSize,
                maxRetryAttempts,
                initialRetryDelayMs
        );
    }

    public long writeTrafficAggregates(Dataset<Row> dataset, UUID batchId) {
        return write(dataset, batchId, "traffic_aggregates", (rows, ingestedAt) -> {
            List<TrafficAggregate> aggregates = new ArrayList<>();
            while (rows.hasNext()) {
                aggregates.add(rowMapper.toTrafficAggregate(rows.next(), batchId, ingestedAt));
            }
            return batchExecutor.executeWithRetry(
                    () -> trafficDao.upsertBatch(aggregates),
                    "traffic_aggregates",
                    maxRetryAttempts,
                    initialRetryDelayMs
            );
        });
    }

    public long writeConnectionAggregates(Dataset<Row> dataset, UUID batchId) {
        return write(dataset, batchId, "connection_aggregates", (rows, ingestedAt) -> {
            List<ConnectionAggregate> aggregates = new ArrayList<>();
            while (rows.hasNext()) {
                aggregates.add(rowMapper.toConnectionAggregate(rows.next(), batchId, ingestedAt));
            }
            return batchExecutor.executeWithRetry(
                    () -> connectionDao.upsertBatch(aggregates),
                    "connection_aggregates",
                    maxRetryAttempts,
                    initialRetryDelayMs
            );
        });
    }

    public long writeProtocolAggregates(Dataset<Row> dataset, UUID batchId) {
        return write(dataset, batchId, "protocol_usage_aggregates", (rows, ingestedAt) -> {
            List<ProtocolAggregate> aggregates = new ArrayList<>();
            while (rows.hasNext()) {
                aggregates.add(rowMapper.toProtocolAggregate(rows.next(), batchId, ingestedAt));
            }
            return batchExecutor.executeWithRetry(
                    () -> protocolDao.upsertBatch(aggregates),
                    "protocol_usage_aggregates",
                    maxRetryAttempts,
                    initialRetryDelayMs
            );
        });
    }

    public long writeIpBandwidthAggregates(Dataset<Row> dataset, UUID batchId) {
        return write(dataset, batchId, "ip_bandwidth_aggregates", (rows, ingestedAt) -> {
            List<TopIpAggregate> aggregates = new ArrayList<>();
            while (rows.hasNext()) {
                aggregates.add(rowMapper.toIpBandwidthAggregate(rows.next(), batchId, ingestedAt));
            }
            return batchExecutor.executeWithRetry(
                    () -> ipBandwidthDao.upsertBatch(aggregates),
                    "ip_bandwidth_aggregates",
                    maxRetryAttempts,
                    initialRetryDelayMs
            );
        });
    }

    private long write(Dataset<Row> dataset, UUID batchId, String tableName, WriteOperation operation) {
        Timestamp ingestedAt = Timestamp.from(Instant.now());
        Iterator<Row> rows = dataset.toLocalIterator();

        if (!rows.hasNext()) {
            log.info("No records to write to table: {}", tableName);
            return 0;
        }

        try {
            int[] results = operation.execute(rows, ingestedAt);
            long totalWritten = batchExecutor.sumResults(results);
            log.info("Wrote {} records to table: {} with batch_id: {}", totalWritten, tableName, batchId);
            return totalWritten;
        } catch (Exception e) {
            log.error("Database write failed for table: {}, batch_id: {}", tableName, batchId, e);
            throw new RuntimeException("Database write failed for table: " + tableName, e);
        }
    }

    @FunctionalInterface
    private interface WriteOperation {
        int[] execute(Iterator<Row> rows, Timestamp ingestedAt);
    }
}