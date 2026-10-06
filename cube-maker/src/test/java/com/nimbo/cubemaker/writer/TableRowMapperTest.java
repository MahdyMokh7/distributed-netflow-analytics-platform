package com.nimbo.cubemaker.writer;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

class TableRowMapperTest {

  private static final UUID BATCH_ID = UUID.fromString("30000000-0000-0000-0000-000000000001");
  private static final Timestamp INGESTED_AT =
      Timestamp.from(Instant.parse("2026-08-29T10:00:00Z"));

  private final TableRowMapper mapper = new TableRowMapper();

  @Test
  void givenAnIpPairAggregateRow_whenMapped_thenItPreservesTheSimplifiedConnectionCube() {
    // Given: a complete source-destination aggregate without port or protocol columns
    Row row =
        row(
            new StructType()
                .add("id", DataTypes.StringType, false)
                .add("src_ip", DataTypes.StringType, false)
                .add("dst_ip", DataTypes.StringType, false)
                .add("window_start", DataTypes.TimestampType, false)
                .add("window_end", DataTypes.TimestampType, false)
                .add("in_bytes", DataTypes.LongType, false)
                .add("out_bytes", DataTypes.LongType, false)
                .add("in_packets", DataTypes.LongType, false)
                .add("out_packets", DataTypes.LongType, false),
            "a".repeat(64),
            "10.0.0.1",
            "10.0.0.2",
            timestamp("2026-08-29T08:00:00Z"),
            timestamp("2026-08-29T09:00:00Z"),
            400L,
            600L,
            4L,
            6L);

    // When: the connection row is mapped for persistence
    var aggregate = mapper.toConnectionAggregate(row, BATCH_ID, INGESTED_AT);

    // Then: the aggregate keeps its metrics, window, and exactly-once batch identity
    assertThat(aggregate.getId()).isEqualTo("a".repeat(64));
    assertThat(aggregate.getSrcIp()).isEqualTo("10.0.0.1");
    assertThat(aggregate.getDstIp()).isEqualTo("10.0.0.2");
    assertThat(aggregate.getInBytes()).isEqualTo(400L);
    assertThat(aggregate.getOutBytes()).isEqualTo(600L);
    assertThat(aggregate.getBatchId()).isEqualTo(BATCH_ID);
    assertThat(aggregate.getIngestedAt()).isEqualTo(INGESTED_AT.toLocalDateTime());
  }

  @Test
  void givenAggregateRowsForTheOtherCubes_whenMapped_thenEachDomainMetricIsPreserved() {
    // Given: complete traffic, protocol, and IP-bandwidth aggregate rows
    Row traffic =
        row(
            new StructType()
                .add("src_ip", DataTypes.StringType, false)
                .add("window_start", DataTypes.TimestampType, false)
                .add("window_end", DataTypes.TimestampType, false)
                .add("in_bytes", DataTypes.LongType, false)
                .add("out_bytes", DataTypes.LongType, false)
                .add("in_packets", DataTypes.LongType, false)
                .add("out_packets", DataTypes.LongType, false)
                .add("record_count", DataTypes.LongType, false),
            "10.0.0.1",
            timestamp("2026-08-29T08:00:00Z"),
            timestamp("2026-08-29T09:00:00Z"),
            100L,
            200L,
            1L,
            2L,
            3L);
    Row protocol =
        row(
            new StructType()
                .add("l7_protocol", DataTypes.DoubleType, false)
                .add("window_start", DataTypes.TimestampType, false)
                .add("window_end", DataTypes.TimestampType, false)
                .add("in_bytes", DataTypes.LongType, false)
                .add("out_bytes", DataTypes.LongType, false)
                .add("in_packets", DataTypes.LongType, false)
                .add("out_packets", DataTypes.LongType, false)
                .add("connection_count", DataTypes.LongType, false),
            6.0,
            timestamp("2026-08-29T08:00:00Z"),
            timestamp("2026-08-29T09:00:00Z"),
            700L,
            800L,
            7L,
            8L,
            9L);
    Row bandwidth =
        row(
            new StructType()
                .add("ip", DataTypes.StringType, false)
                .add("window_start", DataTypes.TimestampType, false)
                .add("window_end", DataTypes.TimestampType, false)
                .add("inbound_bytes", DataTypes.LongType, false)
                .add("outbound_bytes", DataTypes.LongType, false)
                .add("inbound_packets", DataTypes.LongType, false)
                .add("outbound_packets", DataTypes.LongType, false),
            "10.0.0.2",
            timestamp("2026-08-29T08:00:00Z"),
            timestamp("2026-08-29T09:00:00Z"),
            900L,
            1000L,
            9L,
            10L);

    // When: each row is mapped using the real table mapper
    var trafficAggregate = mapper.toTrafficAggregate(traffic, BATCH_ID, INGESTED_AT);
    var protocolAggregate = mapper.toProtocolAggregate(protocol, BATCH_ID, INGESTED_AT);
    var bandwidthAggregate = mapper.toIpBandwidthAggregate(bandwidth, BATCH_ID, INGESTED_AT);

    // Then: cube-specific values and the common batch identity are retained
    assertThat(trafficAggregate.getRecordCount()).isEqualTo(3L);
    assertThat(protocolAggregate.getL7Protocol()).isEqualTo(6.0);
    assertThat(protocolAggregate.getConnectionCount()).isEqualTo(9L);
    assertThat(bandwidthAggregate.getInboundBytes()).isEqualTo(900L);
    assertThat(bandwidthAggregate.getOutboundBytes()).isEqualTo(1000L);
    assertThat(bandwidthAggregate.getBatchId()).isEqualTo(BATCH_ID);
  }

  private Row row(StructType schema, Object... values) {
    return new org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema(values, schema);
  }

  private Timestamp timestamp(String value) {
    return Timestamp.from(Instant.parse(value));
  }
}
