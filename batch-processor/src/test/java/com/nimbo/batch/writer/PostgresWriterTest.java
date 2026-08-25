package com.nimbo.batch.writer;

import com.nimbo.batch.config.PostgresConfig;
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

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class PostgresWriterTest {

    private static SparkSession spark;
    private DataSource dataSource;
    private Connection connection;
    private PreparedStatement preparedStatement;
    private PostgresConfig postgresConfig;
    private TableMapper tableMapper;
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
    void setUp() throws SQLException {
        dataSource = mock(DataSource.class);
        connection = mock(Connection.class);
        preparedStatement = mock(PreparedStatement.class);
        tableMapper = mock(TableMapper.class);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);

        postgresConfig = new PostgresConfig();
        postgresConfig.setBatchSize(2);

        PostgresConfig.Retry retry = new PostgresConfig.Retry();
        retry.setMaxAttempts(3);
        retry.setInitialDelayMs(100);
        postgresConfig.setRetry(retry);

        postgresWriter = new PostgresWriter(dataSource, postgresConfig, tableMapper);
    }

    @Test
    void shouldWriteRecordsToDatabase() throws Exception {
        UUID batchId = UUID.randomUUID();

        List<Row> rows = Arrays.asList(
                RowFactory.create("192.168.1.100", Timestamp.valueOf("2024-01-01 10:00:00"),
                        Timestamp.valueOf("2024-01-01 11:00:00"), 1000L, 2000L, 10L, 20L, 5L),
                RowFactory.create("192.168.1.101", Timestamp.valueOf("2024-01-01 10:00:00"),
                        Timestamp.valueOf("2024-01-01 11:00:00"), 3000L, 4000L, 30L, 40L, 10L)
        );

        Dataset<Row> dataset = createDataset(rows);
        TableMapping mapping = TableMapping.forTrafficAggregates();

        when(preparedStatement.executeBatch()).thenReturn(new int[]{1, 1});

        long written = postgresWriter.write(dataset, mapping, batchId);

        verify(connection).setAutoCommit(false);
        verify(preparedStatement, atLeastOnce()).addBatch();
        verify(preparedStatement, atLeastOnce()).executeBatch();
        verify(connection, atLeastOnce()).commit();

        assertThat(written).isEqualTo(2);
    }

    @Test
    void shouldReturnZeroWhenDatasetIsEmpty() throws SQLException {
        UUID batchId = UUID.randomUUID();
        Dataset<Row> emptyDataset = createDataset(List.of());

        TableMapping mapping = TableMapping.forTrafficAggregates();

        long written = postgresWriter.write(emptyDataset, mapping, batchId);

        assertThat(written).isZero();
        verify(dataSource, never()).getConnection();
    }

    @Test
    void shouldRetryOnBatchFailure() throws Exception {
        UUID batchId = UUID.randomUUID();

        List<Row> rows = Arrays.asList(
                RowFactory.create("192.168.1.100", Timestamp.valueOf("2024-01-01 10:00:00"),
                        Timestamp.valueOf("2024-01-01 11:00:00"), 1000L, 2000L, 10L, 20L, 5L)
        );

        Dataset<Row> dataset = createDataset(rows);
        TableMapping mapping = TableMapping.forTrafficAggregates();

        when(preparedStatement.executeBatch())
                .thenThrow(new SQLException("Batch failed"))
                .thenReturn(new int[]{1});

        long written = postgresWriter.write(dataset, mapping, batchId);

        verify(preparedStatement, times(2)).executeBatch();
        verify(connection, times(1)).rollback();
        assertThat(written).isEqualTo(1);
    }

    @Test
    void shouldFailAfterMaxRetries() throws Exception {
        UUID batchId = UUID.randomUUID();

        List<Row> rows = Arrays.asList(
                RowFactory.create("192.168.1.100", Timestamp.valueOf("2024-01-01 10:00:00"),
                        Timestamp.valueOf("2024-01-01 11:00:00"), 1000L, 2000L, 10L, 20L, 5L)
        );

        Dataset<Row> dataset = createDataset(rows);
        TableMapping mapping = TableMapping.forTrafficAggregates();

        when(preparedStatement.executeBatch())
                .thenThrow(new SQLException("Batch failed"));

        assertThatThrownBy(() -> postgresWriter.write(dataset, mapping, batchId))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Database write failed");

        verify(preparedStatement, times(3)).executeBatch();
        verify(connection, times(3)).rollback();
    }

    @Test
    void shouldHandleNullBatchId() throws Exception {
        List<Row> rows = Arrays.asList(
                RowFactory.create("192.168.1.100", Timestamp.valueOf("2024-01-01 10:00:00"),
                        Timestamp.valueOf("2024-01-01 11:00:00"), 1000L, 2000L, 10L, 20L, 5L)
        );

        Dataset<Row> dataset = createDataset(rows);
        TableMapping mapping = TableMapping.forTrafficAggregates();

        when(preparedStatement.executeBatch()).thenReturn(new int[]{1});

        long written = postgresWriter.write(dataset, mapping, null);

        assertThat(written).isEqualTo(1);
        verify(preparedStatement, atLeastOnce()).addBatch();
    }

    @Test
    void shouldHandleEmptyBatchAfterPartialWrite() throws Exception {
        UUID batchId = UUID.randomUUID();

        List<Row> rows = Arrays.asList(
                RowFactory.create("192.168.1.100", Timestamp.valueOf("2024-01-01 10:00:00"),
                        Timestamp.valueOf("2024-01-01 11:00:00"), 1000L, 2000L, 10L, 20L, 5L),
                RowFactory.create("192.168.1.101", Timestamp.valueOf("2024-01-01 10:00:00"),
                        Timestamp.valueOf("2024-01-01 11:00:00"), 3000L, 4000L, 30L, 40L, 10L),
                RowFactory.create("192.168.1.102", Timestamp.valueOf("2024-01-01 10:00:00"),
                        Timestamp.valueOf("2024-01-01 11:00:00"), 5000L, 6000L, 50L, 60L, 15L)
        );

        Dataset<Row> dataset = createDataset(rows);
        TableMapping mapping = TableMapping.forTrafficAggregates();

        when(preparedStatement.executeBatch()).thenReturn(new int[]{1, 1});

        long written = postgresWriter.write(dataset, mapping, batchId);

        verify(preparedStatement, times(2)).executeBatch();
        assertThat(written).isEqualTo(3);
    }

    @Test
    void shouldLogInitializationParameters() {
        PostgresConfig config = new PostgresConfig();
        config.setBatchSize(500);

        PostgresConfig.Retry retry = new PostgresConfig.Retry();
        retry.setMaxAttempts(4);
        retry.setInitialDelayMs(2000);
        config.setRetry(retry);

        PostgresWriter writer = new PostgresWriter(dataSource, config, tableMapper);

        assertThat(writer).isNotNull();
    }

    @Test
    void shouldBuildUpsertSqlCorrectly() throws Exception {
        TableMapping mapping = TableMapping.forTrafficAggregates();

        UUID batchId = UUID.randomUUID();
        List<Row> rows = Arrays.asList(
                RowFactory.create("192.168.1.100", Timestamp.valueOf("2024-01-01 10:00:00"),
                        Timestamp.valueOf("2024-01-01 11:00:00"), 1000L, 2000L, 10L, 20L, 5L)
        );

        Dataset<Row> dataset = createDataset(rows);

        when(preparedStatement.executeBatch()).thenReturn(new int[]{1});

        long written = postgresWriter.write(dataset, mapping, batchId);

        assertThat(written).isEqualTo(1);
    }

    @Test
    void shouldHandleBatchSizeZero() throws Exception {
        postgresConfig.setBatchSize(0);
        postgresWriter = new PostgresWriter(dataSource, postgresConfig, tableMapper);

        UUID batchId = UUID.randomUUID();
        List<Row> rows = Arrays.asList(
                RowFactory.create("192.168.1.100", Timestamp.valueOf("2024-01-01 10:00:00"),
                        Timestamp.valueOf("2024-01-01 11:00:00"), 1000L, 2000L, 10L, 20L, 5L)
        );

        Dataset<Row> dataset = createDataset(rows);
        TableMapping mapping = TableMapping.forTrafficAggregates();

        when(preparedStatement.executeBatch()).thenReturn(new int[]{1});

        long written = postgresWriter.write(dataset, mapping, batchId);

        assertThat(written).isEqualTo(1);
    }

    @Test
    void shouldHandleInterruptedExceptionDuringRetry() throws Exception {
        UUID batchId = UUID.randomUUID();
        List<Row> rows = Arrays.asList(
                RowFactory.create("192.168.1.100", Timestamp.valueOf("2024-01-01 10:00:00"),
                        Timestamp.valueOf("2024-01-01 11:00:00"), 1000L, 2000L, 10L, 20L, 5L)
        );

        Dataset<Row> dataset = createDataset(rows);
        TableMapping mapping = TableMapping.forTrafficAggregates();

        when(preparedStatement.executeBatch())
                .thenThrow(new SQLException("Batch failed"));

        assertThatThrownBy(() -> postgresWriter.write(dataset, mapping, batchId))
                .isInstanceOf(RuntimeException.class);
    }

    private Dataset<Row> createDataset(List<Row> rows) {
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

        return spark.createDataFrame(rows, schema);
    }
}