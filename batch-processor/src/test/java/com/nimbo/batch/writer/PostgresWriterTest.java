package com.nimbo.batch.writer;

import com.nimbo.batch.config.PostgresConfig;
import com.nimbo.batch.dao.ConnectionAggregateDao;
import com.nimbo.batch.dao.IpBandwidthAggregateDao;
import com.nimbo.batch.dao.ProtocolAggregateDao;
import com.nimbo.batch.dao.TrafficAggregateDao;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

class PostgresWriterTest {

    private static SparkSession spark;
    private TrafficAggregateDao trafficDao;
    private ConnectionAggregateDao connectionDao;
    private ProtocolAggregateDao protocolDao;
    private IpBandwidthAggregateDao ipBandwidthDao;
    private TableRowMapper rowMapper;
    private BatchExecutor batchExecutor;
    private PostgresConfig postgresConfig;
    private PostgresWriter postgresWriter;

    @BeforeAll
    static void setupSpark() {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("postgres-writer-test")
                .getOrCreate();
    }

    @AfterAll
    static void tearDownSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    @BeforeEach
    void setUp() {
        trafficDao = mock(TrafficAggregateDao.class);
        connectionDao = mock(ConnectionAggregateDao.class);
        protocolDao = mock(ProtocolAggregateDao.class);
        ipBandwidthDao = mock(IpBandwidthAggregateDao.class);
        rowMapper = new TableRowMapper();
        batchExecutor = new BatchExecutor();

        postgresConfig = new PostgresConfig();
        postgresConfig.setBatchSize(2);

        PostgresConfig.Retry retry = new PostgresConfig.Retry();
        retry.setMaxAttempts(3);
        retry.setInitialDelayMs(100);
        postgresConfig.setRetry(retry);

        postgresWriter = new PostgresWriter(
                trafficDao, connectionDao, protocolDao, ipBandwidthDao,
                rowMapper, batchExecutor, postgresConfig
        );
    }

    @Test
    void shouldWriteTrafficAggregates() {
        UUID batchId = UUID.randomUUID();
        Dataset<Row> dataset = createTrafficDataset();

        when(trafficDao.upsertBatch(anyList())).thenReturn(new int[]{1, 1});

        long written = postgresWriter.writeTrafficAggregates(dataset, batchId);

        assertThat(written).isEqualTo(2);
        verify(trafficDao).upsertBatch(anyList());
    }

    @Test
    void shouldWriteConnectionAggregates() {
        UUID batchId = UUID.randomUUID();
        Dataset<Row> dataset = createConnectionDataset();

        when(connectionDao.upsertBatch(anyList())).thenReturn(new int[]{1, 1});

        long written = postgresWriter.writeConnectionAggregates(dataset, batchId);

        assertThat(written).isEqualTo(2);
        verify(connectionDao).upsertBatch(anyList());
    }

    @Test
    void shouldWriteProtocolAggregates() {
        UUID batchId = UUID.randomUUID();
        Dataset<Row> dataset = createProtocolDataset();

        when(protocolDao.upsertBatch(anyList())).thenReturn(new int[]{1, 1});

        long written = postgresWriter.writeProtocolAggregates(dataset, batchId);

        assertThat(written).isEqualTo(2);
        verify(protocolDao).upsertBatch(anyList());
    }

    @Test
    void shouldWriteIpBandwidthAggregates() {
        UUID batchId = UUID.randomUUID();
        Dataset<Row> dataset = createIpBandwidthDataset();

        when(ipBandwidthDao.upsertBatch(anyList())).thenReturn(new int[]{1, 1});

        long written = postgresWriter.writeIpBandwidthAggregates(dataset, batchId);

        assertThat(written).isEqualTo(2);
        verify(ipBandwidthDao).upsertBatch(anyList());
    }

    @Test
    void shouldReturnZeroWhenDatasetIsEmpty() {
        UUID batchId = UUID.randomUUID();
        Dataset<Row> emptyDataset = spark.emptyDataFrame();

        long written = postgresWriter.writeTrafficAggregates(emptyDataset, batchId);

        assertThat(written).isZero();
        verify(trafficDao, never()).upsertBatch(anyList());
    }

    @Test
    void shouldRetryOnBatchFailure() {
        UUID batchId = UUID.randomUUID();
        Dataset<Row> dataset = createTrafficDataset();

        when(trafficDao.upsertBatch(anyList()))
                .thenThrow(new RuntimeException("Batch failed"))
                .thenReturn(new int[]{1, 1});

        long written = postgresWriter.writeTrafficAggregates(dataset, batchId);

        assertThat(written).isEqualTo(2);
        verify(trafficDao, times(2)).upsertBatch(anyList());
    }

    @Test
    void shouldFailAfterMaxRetries() {
        UUID batchId = UUID.randomUUID();
        Dataset<Row> dataset = createTrafficDataset();

        when(trafficDao.upsertBatch(anyList()))
                .thenThrow(new RuntimeException("Batch failed"));

        assertThatThrownBy(() -> postgresWriter.writeTrafficAggregates(dataset, batchId))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Database write failed for table: traffic_aggregates")
                .hasCauseInstanceOf(RuntimeException.class)
                .cause()
                .hasMessageContaining("failed after 3 attempts");
    }

    @Test
    void shouldVerifyAllRowsAffected() {
        UUID batchId = UUID.randomUUID();
        Dataset<Row> dataset = createTrafficDataset();

        when(trafficDao.upsertBatch(anyList())).thenReturn(new int[]{1, 1, 1});

        long written = postgresWriter.writeTrafficAggregates(dataset, batchId);

        assertThat(written).isEqualTo(3);
        verify(trafficDao).upsertBatch(anyList());
    }

    @Test
    void shouldHandleInterruptedExceptionDuringRetry() {
        UUID batchId = UUID.randomUUID();
        Dataset<Row> dataset = createTrafficDataset();

        when(trafficDao.upsertBatch(anyList()))
                .thenThrow(new RuntimeException("Batch failed"));

        assertThatThrownBy(() -> postgresWriter.writeTrafficAggregates(dataset, batchId))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void shouldLogInitializationParameters() {
        PostgresConfig config = new PostgresConfig();
        config.setBatchSize(500);

        PostgresConfig.Retry retry = new PostgresConfig.Retry();
        retry.setMaxAttempts(4);
        retry.setInitialDelayMs(2000);
        config.setRetry(retry);

        PostgresWriter writer = new PostgresWriter(
                trafficDao, connectionDao, protocolDao, ipBandwidthDao,
                rowMapper, batchExecutor, config
        );

        assertThat(writer).isNotNull();
    }

    @Test
    void shouldHandleBatchSizeZero() {
        postgresConfig.setBatchSize(0);
        UUID batchId = UUID.randomUUID();
        Dataset<Row> dataset = createTrafficDataset();

        when(trafficDao.upsertBatch(anyList())).thenReturn(new int[]{1, 1});

        long written = postgresWriter.writeTrafficAggregates(dataset, batchId);

        assertThat(written).isEqualTo(2);
        verify(trafficDao).upsertBatch(anyList());
    }

    private Dataset<Row> createTrafficDataset() {
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("src_ip", DataTypes.StringType, false),
                DataTypes.createStructField("window_start", DataTypes.TimestampType, false),
                DataTypes.createStructField("window_end", DataTypes.TimestampType, false),
                DataTypes.createStructField("in_bytes", DataTypes.LongType, false),
                DataTypes.createStructField("out_bytes", DataTypes.LongType, false),
                DataTypes.createStructField("in_packets", DataTypes.LongType, false),
                DataTypes.createStructField("out_packets", DataTypes.LongType, false),
                DataTypes.createStructField("record_count", DataTypes.LongType, false)
        });

        List<Row> rows = Arrays.asList(
                RowFactory.create("192.168.1.100", Timestamp.valueOf("2024-01-01 10:00:00"),
                        Timestamp.valueOf("2024-01-01 11:00:00"), 1000L, 2000L, 10L, 20L, 5L),
                RowFactory.create("192.168.1.101", Timestamp.valueOf("2024-01-01 10:00:00"),
                        Timestamp.valueOf("2024-01-01 11:00:00"), 3000L, 4000L, 30L, 40L, 10L)
        );

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> createConnectionDataset() {
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.StringType, false),
                DataTypes.createStructField("src_ip", DataTypes.StringType, false),
                DataTypes.createStructField("dst_ip", DataTypes.StringType, false),
                DataTypes.createStructField("src_port", DataTypes.IntegerType, false),
                DataTypes.createStructField("dst_port", DataTypes.IntegerType, false),
                DataTypes.createStructField("l4_protocol", DataTypes.IntegerType, false),  // ← FIXED
                DataTypes.createStructField("window_start", DataTypes.TimestampType, false),
                DataTypes.createStructField("window_end", DataTypes.TimestampType, false),
                DataTypes.createStructField("in_bytes", DataTypes.LongType, false),
                DataTypes.createStructField("out_bytes", DataTypes.LongType, false),
                DataTypes.createStructField("in_packets", DataTypes.LongType, false),
                DataTypes.createStructField("out_packets", DataTypes.LongType, false)
        });

        List<Row> rows = Arrays.asList(
                RowFactory.create("id1", "192.168.1.100", "10.0.0.1", 8080, 443, 6,
                        Timestamp.valueOf("2024-01-01 10:00:00"), Timestamp.valueOf("2024-01-01 11:00:00"),
                        1000L, 2000L, 10L, 20L),
                RowFactory.create("id2", "192.168.1.101", "10.0.0.2", 8081, 444, 17,
                        Timestamp.valueOf("2024-01-01 10:00:00"), Timestamp.valueOf("2024-01-01 11:00:00"),
                        3000L, 4000L, 30L, 40L)
        );

        return spark.createDataFrame(rows, schema);
    }

    private Dataset<Row> createProtocolDataset() {
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("l7_protocol", DataTypes.DoubleType, false),  // ← FIXED
                DataTypes.createStructField("window_start", DataTypes.TimestampType, false),
                DataTypes.createStructField("window_end", DataTypes.TimestampType, false),
                DataTypes.createStructField("in_bytes", DataTypes.LongType, false),
                DataTypes.createStructField("out_bytes", DataTypes.LongType, false),
                DataTypes.createStructField("in_packets", DataTypes.LongType, false),
                DataTypes.createStructField("out_packets", DataTypes.LongType, false),
                DataTypes.createStructField("connection_count", DataTypes.LongType, false)
        });

        List<Row> rows = Arrays.asList(
                RowFactory.create(6.0, Timestamp.valueOf("2024-01-01 10:00:00"),
                        Timestamp.valueOf("2024-01-01 11:00:00"), 1000L, 2000L, 10L, 20L, 100L),
                RowFactory.create(17.0, Timestamp.valueOf("2024-01-01 10:00:00"),
                        Timestamp.valueOf("2024-01-01 11:00:00"), 3000L, 4000L, 30L, 40L, 200L)
        );

        return spark.createDataFrame(rows, schema);
    }

    @Test
    void shouldThrowWhenBatchResultIsNotOne() {
        UUID batchId = UUID.randomUUID();
        Dataset<Row> dataset = createTrafficDataset();

        when(trafficDao.upsertBatch(anyList())).thenReturn(new int[]{0, 1});

        assertThatThrownBy(() -> postgresWriter.writeTrafficAggregates(dataset, batchId))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Database write failed for table: traffic_aggregates")
                .hasCauseInstanceOf(RuntimeException.class)
                .extracting(Throwable::getCause)
                .satisfies(cause -> {
                    assertThat(cause.getMessage()).contains("Batch execution failed after 3 attempts for table: traffic_aggregates");
                    assertThat(cause.getCause())
                            .isInstanceOf(RuntimeException.class)
                            .hasMessageContaining("expected 1 row affected, got 0");
                });
    }

    private Dataset<Row> createIpBandwidthDataset() {
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("ip", DataTypes.StringType, false),
                DataTypes.createStructField("window_start", DataTypes.TimestampType, false),
                DataTypes.createStructField("window_end", DataTypes.TimestampType, false),
                DataTypes.createStructField("inbound_bytes", DataTypes.LongType, false),
                DataTypes.createStructField("outbound_bytes", DataTypes.LongType, false),
                DataTypes.createStructField("inbound_packets", DataTypes.LongType, false),
                DataTypes.createStructField("outbound_packets", DataTypes.LongType, false)
        });

        List<Row> rows = Arrays.asList(
                RowFactory.create("192.168.1.100", Timestamp.valueOf("2024-01-01 10:00:00"),
                        Timestamp.valueOf("2024-01-01 11:00:00"), 1000L, 2000L, 10L, 20L),
                RowFactory.create("192.168.1.101", Timestamp.valueOf("2024-01-01 10:00:00"),
                        Timestamp.valueOf("2024-01-01 11:00:00"), 3000L, 4000L, 30L, 40L)
        );

        return spark.createDataFrame(rows, schema);
    }
}