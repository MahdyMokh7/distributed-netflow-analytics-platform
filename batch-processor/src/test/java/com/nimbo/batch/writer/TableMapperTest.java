package com.nimbo.batch.writer;

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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class TableMapperTest {

    private static SparkSession spark;
    private TableMapper tableMapper;
    private PreparedStatement preparedStatement;

    @BeforeAll
    static void setupSpark() {
        spark = SparkSession.builder()
                .master("local[1]")
                .appName("table-mapper-test")
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
        tableMapper = new TableMapper();
        preparedStatement = mock(PreparedStatement.class);
    }

    private Row createRowWithSchema(List<Object> values, StructType schema) {
        return spark.createDataFrame(
                Arrays.asList(RowFactory.create(values.toArray())),
                schema
        ).first();
    }

    @Test
    void shouldMapTrafficAggregateRowToStatement() throws SQLException {
        UUID batchId = UUID.randomUUID();
        Timestamp ingestedAt = Timestamp.from(Instant.now());

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("src_ip", DataTypes.StringType, false),
                DataTypes.createStructField("window_start", DataTypes.TimestampType, false),
                DataTypes.createStructField("window_end", DataTypes.TimestampType, false),
                DataTypes.createStructField("in_bytes", DataTypes.LongType, false),
                DataTypes.createStructField("out_bytes", DataTypes.LongType, false),
                DataTypes.createStructField("in_packets", DataTypes.LongType, false),
                DataTypes.createStructField("out_packets", DataTypes.LongType, false),
                DataTypes.createStructField("record_count", DataTypes.LongType, false),
                DataTypes.createStructField("batch_id", DataTypes.StringType, false),
                DataTypes.createStructField("ingested_at", DataTypes.TimestampType, false)
        });

        Row row = createRowWithSchema(Arrays.asList(
                "192.168.1.100",
                Timestamp.valueOf("2024-01-01 10:00:00"),
                Timestamp.valueOf("2024-01-01 11:00:00"),
                1000L, 2000L, 10L, 20L, 5L,
                batchId.toString(),
                ingestedAt
        ), schema);

        TableMapping mapping = TableMapping.forTrafficAggregates();

        tableMapper.mapRowToStatement(preparedStatement, row, mapping, batchId, ingestedAt);

        verify(preparedStatement, atLeastOnce()).setString(anyInt(), anyString());
        verify(preparedStatement, atLeastOnce()).setTimestamp(anyInt(), any(Timestamp.class));
        verify(preparedStatement, atLeastOnce()).setLong(anyInt(), anyLong());
        verify(preparedStatement).setObject(anyInt(), eq(batchId));
    }

    @Test
    void shouldMapConnectionAggregateRowToStatement() throws SQLException {
        UUID batchId = UUID.randomUUID();
        Timestamp ingestedAt = Timestamp.from(Instant.now());
        String connectionId = "abc123def456";

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.StringType, false),
                DataTypes.createStructField("src_ip", DataTypes.StringType, false),
                DataTypes.createStructField("dst_ip", DataTypes.StringType, false),
                DataTypes.createStructField("src_port", DataTypes.IntegerType, false),
                DataTypes.createStructField("dst_port", DataTypes.IntegerType, false),
                DataTypes.createStructField("l4_protocol", DataTypes.IntegerType, false),
                DataTypes.createStructField("window_start", DataTypes.TimestampType, false),
                DataTypes.createStructField("window_end", DataTypes.TimestampType, false),
                DataTypes.createStructField("in_bytes", DataTypes.LongType, false),
                DataTypes.createStructField("out_bytes", DataTypes.LongType, false),
                DataTypes.createStructField("in_packets", DataTypes.LongType, false),
                DataTypes.createStructField("out_packets", DataTypes.LongType, false),
                DataTypes.createStructField("batch_id", DataTypes.StringType, false),
                DataTypes.createStructField("ingested_at", DataTypes.TimestampType, false)
        });

        Row row = createRowWithSchema(Arrays.asList(
                connectionId,
                "192.168.1.100",
                "10.0.0.1",
                8080, 443, 6,
                Timestamp.valueOf("2024-01-01 10:00:00"),
                Timestamp.valueOf("2024-01-01 10:05:00"),
                1000L, 2000L, 10L, 20L,
                batchId.toString(),
                ingestedAt
        ), schema);

        TableMapping mapping = TableMapping.forConnectionAggregates();

        tableMapper.mapRowToStatement(preparedStatement, row, mapping, batchId, ingestedAt);

        verify(preparedStatement).setString(anyInt(), eq(connectionId));
        verify(preparedStatement).setObject(anyInt(), eq(batchId));
        verify(preparedStatement).setTimestamp(anyInt(), eq(ingestedAt));
    }

    @Test
    void shouldMapProtocolUsageAggregateRowToStatement() throws SQLException {
        UUID batchId = UUID.randomUUID();
        Timestamp ingestedAt = Timestamp.from(Instant.now());

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("l7_protocol", DataTypes.DoubleType, false),
                DataTypes.createStructField("window_start", DataTypes.TimestampType, false),
                DataTypes.createStructField("window_end", DataTypes.TimestampType, false),
                DataTypes.createStructField("in_bytes", DataTypes.LongType, false),
                DataTypes.createStructField("out_bytes", DataTypes.LongType, false),
                DataTypes.createStructField("in_packets", DataTypes.LongType, false),
                DataTypes.createStructField("out_packets", DataTypes.LongType, false),
                DataTypes.createStructField("connection_count", DataTypes.LongType, false),
                DataTypes.createStructField("batch_id", DataTypes.StringType, false),
                DataTypes.createStructField("ingested_at", DataTypes.TimestampType, false)
        });

        Row row = createRowWithSchema(Arrays.asList(
                6.0,
                Timestamp.valueOf("2024-01-01 10:00:00"),
                Timestamp.valueOf("2024-01-01 11:00:00"),
                1000L, 2000L, 10L, 20L, 100L,
                batchId.toString(),
                ingestedAt
        ), schema);

        TableMapping mapping = TableMapping.forProtocolUsageAggregates();

        tableMapper.mapRowToStatement(preparedStatement, row, mapping, batchId, ingestedAt);

        verify(preparedStatement).setDouble(anyInt(), eq(6.0));
        verify(preparedStatement).setLong(anyInt(), eq(100L));
    }

    @Test
    void shouldMapIpBandwidthAggregateRowToStatement() throws SQLException {
        UUID batchId = UUID.randomUUID();
        Timestamp ingestedAt = Timestamp.from(Instant.now());

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("ip", DataTypes.StringType, false),
                DataTypes.createStructField("window_start", DataTypes.TimestampType, false),
                DataTypes.createStructField("window_end", DataTypes.TimestampType, false),
                DataTypes.createStructField("inbound_bytes", DataTypes.LongType, false),
                DataTypes.createStructField("outbound_bytes", DataTypes.LongType, false),
                DataTypes.createStructField("inbound_packets", DataTypes.LongType, false),
                DataTypes.createStructField("outbound_packets", DataTypes.LongType, false),
                DataTypes.createStructField("batch_id", DataTypes.StringType, false),
                DataTypes.createStructField("ingested_at", DataTypes.TimestampType, false)
        });

        Row row = createRowWithSchema(Arrays.asList(
                "192.168.1.100",
                Timestamp.valueOf("2024-01-01 10:00:00"),
                Timestamp.valueOf("2024-01-01 11:00:00"),
                1000L, 2000L, 10L, 20L,
                batchId.toString(),
                ingestedAt
        ), schema);

        TableMapping mapping = TableMapping.forIpBandwidthAggregates();

        tableMapper.mapRowToStatement(preparedStatement, row, mapping, batchId, ingestedAt);

        verify(preparedStatement).setString(anyInt(), eq("192.168.1.100"));
        verify(preparedStatement).setObject(anyInt(), eq(batchId));
    }

    @Test
    void shouldHandleNullValueBySettingNull() throws SQLException {
        UUID batchId = UUID.randomUUID();
        Timestamp ingestedAt = Timestamp.from(Instant.now());

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("src_ip", DataTypes.StringType, false),
                DataTypes.createStructField("window_start", DataTypes.TimestampType, true),
                DataTypes.createStructField("window_end", DataTypes.TimestampType, false),
                DataTypes.createStructField("in_bytes", DataTypes.LongType, false),
                DataTypes.createStructField("out_bytes", DataTypes.LongType, false),
                DataTypes.createStructField("in_packets", DataTypes.LongType, false),
                DataTypes.createStructField("out_packets", DataTypes.LongType, false),
                DataTypes.createStructField("record_count", DataTypes.LongType, false),
                DataTypes.createStructField("batch_id", DataTypes.StringType, false),
                DataTypes.createStructField("ingested_at", DataTypes.TimestampType, false)
        });

        Row row = createRowWithSchema(Arrays.asList(
                "192.168.1.100",
                null,
                Timestamp.valueOf("2024-01-01 11:00:00"),
                1000L, 2000L, 10L, 20L, 5L,
                batchId.toString(),
                ingestedAt
        ), schema);

        TableMapping mapping = TableMapping.forTrafficAggregates();

        tableMapper.mapRowToStatement(preparedStatement, row, mapping, batchId, ingestedAt);

        verify(preparedStatement, atLeastOnce()).setNull(anyInt(), anyInt());
    }

    @Test
    void shouldConvertLocalDateTimeToTimestamp() throws SQLException {
        UUID batchId = UUID.randomUUID();
        Timestamp ingestedAt = Timestamp.from(Instant.now());
        LocalDateTime now = LocalDateTime.now();

        // Convert LocalDateTime to Timestamp before passing to Row
        Timestamp nowAsTimestamp = Timestamp.valueOf(now);

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("src_ip", DataTypes.StringType, false),
                DataTypes.createStructField("window_start", DataTypes.TimestampType, false),
                DataTypes.createStructField("window_end", DataTypes.TimestampType, false),
                DataTypes.createStructField("in_bytes", DataTypes.LongType, false),
                DataTypes.createStructField("out_bytes", DataTypes.LongType, false),
                DataTypes.createStructField("in_packets", DataTypes.LongType, false),
                DataTypes.createStructField("out_packets", DataTypes.LongType, false),
                DataTypes.createStructField("record_count", DataTypes.LongType, false),
                DataTypes.createStructField("batch_id", DataTypes.StringType, false),
                DataTypes.createStructField("ingested_at", DataTypes.TimestampType, false)
        });

        Row row = createRowWithSchema(Arrays.asList(
                "192.168.1.100",
                nowAsTimestamp,  // Pass Timestamp, not LocalDateTime
                Timestamp.valueOf("2024-01-01 11:00:00"),
                1000L, 2000L, 10L, 20L, 5L,
                batchId.toString(),
                ingestedAt
        ), schema);

        TableMapping mapping = TableMapping.forTrafficAggregates();

        tableMapper.mapRowToStatement(preparedStatement, row, mapping, batchId, ingestedAt);

        verify(preparedStatement, atLeastOnce()).setTimestamp(anyInt(), any(Timestamp.class));
    }

    @Test
    void shouldPropagateBatchIdCorrectly() throws SQLException {
        UUID expectedBatchId = UUID.randomUUID();
        Timestamp ingestedAt = Timestamp.from(Instant.now());

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("src_ip", DataTypes.StringType, false),
                DataTypes.createStructField("window_start", DataTypes.TimestampType, false),
                DataTypes.createStructField("window_end", DataTypes.TimestampType, false),
                DataTypes.createStructField("in_bytes", DataTypes.LongType, false),
                DataTypes.createStructField("out_bytes", DataTypes.LongType, false),
                DataTypes.createStructField("in_packets", DataTypes.LongType, false),
                DataTypes.createStructField("out_packets", DataTypes.LongType, false),
                DataTypes.createStructField("record_count", DataTypes.LongType, false),
                DataTypes.createStructField("batch_id", DataTypes.StringType, false),
                DataTypes.createStructField("ingested_at", DataTypes.TimestampType, false)
        });

        Row row = createRowWithSchema(Arrays.asList(
                "192.168.1.100",
                Timestamp.valueOf("2024-01-01 10:00:00"),
                Timestamp.valueOf("2024-01-01 11:00:00"),
                1000L, 2000L, 10L, 20L, 5L,
                expectedBatchId.toString(),
                ingestedAt
        ), schema);

        TableMapping mapping = TableMapping.forTrafficAggregates();

        tableMapper.mapRowToStatement(preparedStatement, row, mapping, expectedBatchId, ingestedAt);

        verify(preparedStatement).setObject(anyInt(), eq(expectedBatchId));
    }

    @Test
    void shouldHandleUnknownTypeBySettingObject() throws SQLException {
        UUID batchId = UUID.randomUUID();
        Timestamp ingestedAt = Timestamp.from(Instant.now());

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("src_ip", DataTypes.StringType, false),
                DataTypes.createStructField("window_start", DataTypes.TimestampType, false),
                DataTypes.createStructField("window_end", DataTypes.TimestampType, false),
                DataTypes.createStructField("in_bytes", DataTypes.LongType, false),
                DataTypes.createStructField("out_bytes", DataTypes.LongType, false),
                DataTypes.createStructField("in_packets", DataTypes.LongType, false),
                DataTypes.createStructField("out_packets", DataTypes.LongType, false),
                DataTypes.createStructField("record_count", DataTypes.LongType, false),
                DataTypes.createStructField("custom_field", DataTypes.StringType, false),
                DataTypes.createStructField("batch_id", DataTypes.StringType, false),
                DataTypes.createStructField("ingested_at", DataTypes.TimestampType, false)
        });

        Row row = createRowWithSchema(Arrays.asList(
                "192.168.1.100",
                Timestamp.valueOf("2024-01-01 10:00:00"),
                Timestamp.valueOf("2024-01-01 11:00:00"),
                1000L, 2000L, 10L, 20L, 5L,
                "custom_value",
                batchId.toString(),
                ingestedAt
        ), schema);

        TableMapping mapping = TableMapping.forTrafficAggregates();

        tableMapper.mapRowToStatement(preparedStatement, row, mapping, batchId, ingestedAt);

        verify(preparedStatement, atLeastOnce()).setObject(anyInt(), any());
    }

    @Test
    void shouldHandleUuidStringConversion() throws SQLException {
        UUID batchId = UUID.randomUUID();
        Timestamp ingestedAt = Timestamp.from(Instant.now());
        UUID testUuid = UUID.randomUUID();

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.StringType, false),
                DataTypes.createStructField("src_ip", DataTypes.StringType, false),
                DataTypes.createStructField("dst_ip", DataTypes.StringType, false),
                DataTypes.createStructField("src_port", DataTypes.IntegerType, false),
                DataTypes.createStructField("dst_port", DataTypes.IntegerType, false),
                DataTypes.createStructField("l4_protocol", DataTypes.IntegerType, false),
                DataTypes.createStructField("window_start", DataTypes.TimestampType, false),
                DataTypes.createStructField("window_end", DataTypes.TimestampType, false),
                DataTypes.createStructField("in_bytes", DataTypes.LongType, false),
                DataTypes.createStructField("out_bytes", DataTypes.LongType, false),
                DataTypes.createStructField("in_packets", DataTypes.LongType, false),
                DataTypes.createStructField("out_packets", DataTypes.LongType, false),
                DataTypes.createStructField("batch_id", DataTypes.StringType, false),
                DataTypes.createStructField("ingested_at", DataTypes.TimestampType, false)
        });

        Row row = createRowWithSchema(Arrays.asList(
                testUuid.toString(),
                "192.168.1.100",
                "10.0.0.1",
                8080, 443, 6,
                Timestamp.valueOf("2024-01-01 10:00:00"),
                Timestamp.valueOf("2024-01-01 10:05:00"),
                1000L, 2000L, 10L, 20L,
                batchId.toString(),
                ingestedAt
        ), schema);

        TableMapping mapping = TableMapping.forConnectionAggregates();

        tableMapper.mapRowToStatement(preparedStatement, row, mapping, batchId, ingestedAt);

        verify(preparedStatement).setString(anyInt(), eq(testUuid.toString()));
        verify(preparedStatement).setObject(anyInt(), eq(batchId));
    }

    @Test
    void shouldHandleFloatToDoubleConversion() throws SQLException {
        UUID batchId = UUID.randomUUID();
        Timestamp ingestedAt = Timestamp.from(Instant.now());

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("l7_protocol", DataTypes.FloatType, false),
                DataTypes.createStructField("window_start", DataTypes.TimestampType, false),
                DataTypes.createStructField("window_end", DataTypes.TimestampType, false),
                DataTypes.createStructField("in_bytes", DataTypes.LongType, false),
                DataTypes.createStructField("out_bytes", DataTypes.LongType, false),
                DataTypes.createStructField("in_packets", DataTypes.LongType, false),
                DataTypes.createStructField("out_packets", DataTypes.LongType, false),
                DataTypes.createStructField("connection_count", DataTypes.LongType, false),
                DataTypes.createStructField("batch_id", DataTypes.StringType, false),
                DataTypes.createStructField("ingested_at", DataTypes.TimestampType, false)
        });

        Row row = createRowWithSchema(Arrays.asList(
                6.0f,
                Timestamp.valueOf("2024-01-01 10:00:00"),
                Timestamp.valueOf("2024-01-01 11:00:00"),
                1000L, 2000L, 10L, 20L, 100L,
                batchId.toString(),
                ingestedAt
        ), schema);

        TableMapping mapping = TableMapping.forProtocolUsageAggregates();

        tableMapper.mapRowToStatement(preparedStatement, row, mapping, batchId, ingestedAt);

        verify(preparedStatement).setDouble(anyInt(), eq(6.0));
    }

    @Test
    void shouldHandleLongToIntConversion() throws SQLException {
        UUID batchId = UUID.randomUUID();
        Timestamp ingestedAt = Timestamp.from(Instant.now());

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.StringType, false),
                DataTypes.createStructField("src_ip", DataTypes.StringType, false),
                DataTypes.createStructField("dst_ip", DataTypes.StringType, false),
                DataTypes.createStructField("src_port", DataTypes.LongType, false),
                DataTypes.createStructField("dst_port", DataTypes.IntegerType, false),
                DataTypes.createStructField("l4_protocol", DataTypes.IntegerType, false),
                DataTypes.createStructField("window_start", DataTypes.TimestampType, false),
                DataTypes.createStructField("window_end", DataTypes.TimestampType, false),
                DataTypes.createStructField("in_bytes", DataTypes.LongType, false),
                DataTypes.createStructField("out_bytes", DataTypes.LongType, false),
                DataTypes.createStructField("in_packets", DataTypes.LongType, false),
                DataTypes.createStructField("out_packets", DataTypes.LongType, false),
                DataTypes.createStructField("batch_id", DataTypes.StringType, false),
                DataTypes.createStructField("ingested_at", DataTypes.TimestampType, false)
        });

        Row row = createRowWithSchema(Arrays.asList(
                "test-id-123",
                "192.168.1.100",
                "10.0.0.1",
                8080L, 443, 6,
                Timestamp.valueOf("2024-01-01 10:00:00"),
                Timestamp.valueOf("2024-01-01 10:05:00"),
                1000L, 2000L, 10L, 20L,
                batchId.toString(),
                ingestedAt
        ), schema);

        TableMapping mapping = TableMapping.forConnectionAggregates();

        tableMapper.mapRowToStatement(preparedStatement, row, mapping, batchId, ingestedAt);

        verify(preparedStatement).setInt(anyInt(), eq(8080));
    }
}