package com.nimbo.restapi.repository.Starrocks;

import com.nimbo.restapi.client.StarRocksQueryClient;
import com.nimbo.restapi.config.StarRocksQueryProperties;
import com.nimbo.restapi.dto.response.Starrocks.StarrocksBandwidthResponseDto;
import com.nimbo.restapi.dto.response.Starrocks.StarrocksConnectionResponseDto;
import com.nimbo.restapi.dto.response.Starrocks.StarrocksProtocolResponseDto;
import com.nimbo.restapi.dto.response.Starrocks.StarrocksTrafficResponseDto;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class StarrocksQueryRepository {

  private final NamedParameterJdbcTemplate jdbc;
  private final String table;

  public StarrocksQueryRepository(
      StarRocksQueryClient queryClient, StarRocksQueryProperties properties) {
    this.jdbc = queryClient.jdbc();
    this.table = properties.qualifiedTable();
  }

  public List<StarrocksTrafficResponseDto> findTrafficByIp(
      String ip, Instant startTime, Instant endTime) {
    String sql = trafficSql(table);

    MapSqlParameterSource parameters = timeParameters(startTime, endTime).addValue("ip", ip);

    return jdbc.query(
        sql,
        parameters,
        (rs, rowNum) ->
            StarrocksTrafficResponseDto.builder()
                .srcIp(rs.getString("ipv4_src_addr"))
                .windowStart(Instant.ofEpochMilli(rs.getLong("window_start")))
                .windowEnd(Instant.ofEpochMilli(rs.getLong("window_end")))
                .inBytes(bigInteger(rs, "in_bytes"))
                .outBytes(bigInteger(rs, "out_bytes"))
                .totalBytes(bigInteger(rs, "total_bytes"))
                .inPackets(bigInteger(rs, "in_packets"))
                .outPackets(bigInteger(rs, "out_packets"))
                .totalPackets(bigInteger(rs, "total_packets"))
                .recordCount(bigInteger(rs, "record_count"))
                .build());
  }

  static String trafficSql(String table) {
    return """
        WITH hourly_source AS (
            SELECT
                ipv4_src_addr,
                CAST(FLOOR(PROCESS_TIME / 3600000) * 3600000 AS BIGINT) AS window_start,
                in_bytes,
                out_bytes,
                in_pkts,
                out_pkts
            FROM %s
        )
        SELECT
            ipv4_src_addr,
            window_start,
            window_start + 3600000 AS window_end,
            COALESCE(SUM(in_bytes), 0) AS in_bytes,
            COALESCE(SUM(out_bytes), 0) AS out_bytes,
            COALESCE(SUM(in_bytes + out_bytes), 0) AS total_bytes,
            COALESCE(SUM(in_pkts), 0) AS in_packets,
            COALESCE(SUM(out_pkts), 0) AS out_packets,
            COALESCE(SUM(in_pkts + out_pkts), 0) AS total_packets,
            COUNT(*) AS record_count
        FROM hourly_source
        WHERE window_start >= :startTime
          AND window_start < :endTime
          AND ipv4_src_addr = :ip
        GROUP BY ipv4_src_addr, window_start
        ORDER BY window_start DESC
        """
        .formatted(table);
  }

  public Page<StarrocksConnectionResponseDto> findConnectionsByIp(
      String ip, Instant startTime, Instant endTime, int page, int size) {
    String querySql = connectionSql(table);
    String countSql = connectionCountSql(table);

    MapSqlParameterSource parameters =
        timeParameters(startTime, endTime)
            .addValue("ip", ip)
            .addValue("limit", size)
            .addValue("offset", page * size);

    List<StarrocksConnectionResponseDto> rows =
        jdbc.query(querySql, parameters, (rs, rowNum) -> mapConnection(rs));

    Long total =
        jdbc.queryForObject(
            countSql, timeParameters(startTime, endTime).addValue("ip", ip), Long.class);

    return new PageImpl<>(rows, PageRequest.of(page, size), total == null ? 0 : total);
  }

  static String connectionSql(String table) {
    String hourlyCte =
        """
        WITH hourly AS (
            SELECT
                ipv4_src_addr,
                ipv4_dst_addr,
                CAST(FLOOR(PROCESS_TIME / 3600000) * 3600000 AS BIGINT) AS window_start,
                in_bytes,
                out_bytes,
                in_pkts,
                out_pkts
            FROM %s
        )
        """
            .formatted(table);

    return hourlyCte
        + """
            SELECT
                ipv4_src_addr,
                ipv4_dst_addr,
                window_start,
                window_start + 3600000 AS window_end,
                COALESCE(SUM(in_bytes), 0) AS in_bytes,
                COALESCE(SUM(out_bytes), 0) AS out_bytes,
                COALESCE(SUM(in_bytes + out_bytes), 0) AS total_bytes,
                COALESCE(SUM(in_pkts), 0) AS in_packets,
                COALESCE(SUM(out_pkts), 0) AS out_packets,
                COALESCE(SUM(in_pkts + out_pkts), 0) AS total_packets
            FROM hourly
            WHERE window_start >= :startTime
              AND window_start < :endTime
              AND (ipv4_src_addr = :ip OR ipv4_dst_addr = :ip)
            GROUP BY ipv4_src_addr, ipv4_dst_addr, window_start
            ORDER BY window_start DESC, ipv4_src_addr, ipv4_dst_addr
            LIMIT :limit
            OFFSET :offset
            """;
  }

  static String connectionCountSql(String table) {
    return """
        WITH hourly AS (
            SELECT
                ipv4_src_addr,
                ipv4_dst_addr,
                CAST(FLOOR(PROCESS_TIME / 3600000) * 3600000 AS BIGINT) AS window_start
            FROM %s
        )
            SELECT COUNT(*)
            FROM (
                SELECT ipv4_src_addr, ipv4_dst_addr, window_start
                FROM hourly
                WHERE window_start >= :startTime
                  AND window_start < :endTime
                  AND (ipv4_src_addr = :ip OR ipv4_dst_addr = :ip)
                GROUP BY ipv4_src_addr, ipv4_dst_addr, window_start
            ) grouped_connections
        """
        .formatted(table);
  }

  public List<StarrocksBandwidthResponseDto> findTopBandwidthIps(
      Instant startTime, Instant endTime, int limit) {
    String sql = bandwidthSql(table);

    MapSqlParameterSource parameters = timeParameters(startTime, endTime).addValue("limit", limit);

    return jdbc.query(
        sql,
        parameters,
        (rs, rowNum) ->
            StarrocksBandwidthResponseDto.builder()
                .ip(rs.getString("ip"))
                .inboundBytes(bigInteger(rs, "inbound_bytes"))
                .outboundBytes(bigInteger(rs, "outbound_bytes"))
                .totalBytes(bigInteger(rs, "total_bytes"))
                .inboundPackets(bigInteger(rs, "inbound_packets"))
                .outboundPackets(bigInteger(rs, "outbound_packets"))
                .totalPackets(bigInteger(rs, "total_packets"))
                .build());
  }

  static String bandwidthSql(String table) {
    return """
        WITH endpoint_rows AS (
            SELECT
                ipv4_src_addr AS ip,
                CAST(FLOOR(PROCESS_TIME / 3600000) * 3600000 AS BIGINT) AS window_start,
                in_bytes AS inbound_bytes,
                out_bytes AS outbound_bytes,
                in_pkts AS inbound_packets,
                out_pkts AS outbound_packets
            FROM %s

            UNION ALL

            SELECT
                ipv4_dst_addr AS ip,
                CAST(FLOOR(PROCESS_TIME / 3600000) * 3600000 AS BIGINT) AS window_start,
                out_bytes AS inbound_bytes,
                in_bytes AS outbound_bytes,
                out_pkts AS inbound_packets,
                in_pkts AS outbound_packets
            FROM %s
        ), hourly AS (
            SELECT
                ip,
                window_start,
                SUM(inbound_bytes) AS inbound_bytes,
                SUM(outbound_bytes) AS outbound_bytes,
                SUM(inbound_packets) AS inbound_packets,
                SUM(outbound_packets) AS outbound_packets
            FROM endpoint_rows
            WHERE window_start >= :startTime
              AND window_start < :endTime
            GROUP BY ip, window_start
        )
        SELECT
            ip,
            COALESCE(SUM(inbound_bytes), 0) AS inbound_bytes,
            COALESCE(SUM(outbound_bytes), 0) AS outbound_bytes,
            COALESCE(SUM(inbound_bytes + outbound_bytes), 0) AS total_bytes,
            COALESCE(SUM(inbound_packets), 0) AS inbound_packets,
            COALESCE(SUM(outbound_packets), 0) AS outbound_packets,
            COALESCE(SUM(inbound_packets + outbound_packets), 0) AS total_packets
        FROM hourly
        GROUP BY ip
        ORDER BY total_bytes DESC
        LIMIT :limit
        """
        .formatted(table, table);
  }

  public List<StarrocksProtocolResponseDto> findProtocolUsage(
      Instant startTime, Instant endTime) {
    String sql = protocolSql(table);

    return jdbc.query(
        sql,
        timeParameters(startTime, endTime),
        (rs, rowNum) ->
            StarrocksProtocolResponseDto.builder()
                .l7Protocol(rs.getDouble("l7_proto"))
                .windowStart(Instant.ofEpochMilli(rs.getLong("window_start")))
                .windowEnd(Instant.ofEpochMilli(rs.getLong("window_end")))
                .inBytes(bigInteger(rs, "in_bytes"))
                .outBytes(bigInteger(rs, "out_bytes"))
                .totalBytes(bigInteger(rs, "total_bytes"))
                .inPackets(bigInteger(rs, "in_packets"))
                .outPackets(bigInteger(rs, "out_packets"))
                .totalPackets(bigInteger(rs, "total_packets"))
                .connectionCount(rs.getLong("connection_count"))
                .build());
  }

  static String protocolSql(String table) {
    return """
        WITH hourly AS (
            SELECT
                l7_proto,
                CAST(FLOOR(PROCESS_TIME / 3600000) * 3600000 AS BIGINT) AS window_start,
                in_bytes,
                out_bytes,
                in_pkts,
                out_pkts
            FROM %s
        )
        SELECT
            l7_proto,
            window_start,
            window_start + 3600000 AS window_end,
            COALESCE(SUM(in_bytes), 0) AS in_bytes,
            COALESCE(SUM(out_bytes), 0) AS out_bytes,
            COALESCE(SUM(in_bytes + out_bytes), 0) AS total_bytes,
            COALESCE(SUM(in_pkts), 0) AS in_packets,
            COALESCE(SUM(out_pkts), 0) AS out_packets,
            COALESCE(SUM(in_pkts + out_pkts), 0) AS total_packets,
            COUNT(*) AS connection_count
        FROM hourly
        WHERE window_start >= :startTime
          AND window_start < :endTime
        GROUP BY l7_proto, window_start
        ORDER BY l7_proto, window_start DESC
        """
        .formatted(table);
  }

  private StarrocksConnectionResponseDto mapConnection(ResultSet rs) throws SQLException {
    return StarrocksConnectionResponseDto.builder()
        .srcIp(rs.getString("ipv4_src_addr"))
        .dstIp(rs.getString("ipv4_dst_addr"))
        .windowStart(Instant.ofEpochMilli(rs.getLong("window_start")))
        .windowEnd(Instant.ofEpochMilli(rs.getLong("window_end")))
        .inBytes(bigInteger(rs, "in_bytes"))
        .outBytes(bigInteger(rs, "out_bytes"))
        .totalBytes(bigInteger(rs, "total_bytes"))
        .inPackets(bigInteger(rs, "in_packets"))
        .outPackets(bigInteger(rs, "out_packets"))
        .totalPackets(bigInteger(rs, "total_packets"))
        .build();
  }

  private MapSqlParameterSource timeParameters(Instant startTime, Instant endTime) {
    return new MapSqlParameterSource()
        .addValue("startTime", startTime.toEpochMilli())
        .addValue("endTime", endTime.toEpochMilli());
  }

  private BigInteger bigInteger(ResultSet resultSet, String column) throws SQLException {
    Object value = resultSet.getObject(column);
    if (value == null) {
      return BigInteger.ZERO;
    }
    if (value instanceof BigInteger bigInteger) {
      return bigInteger;
    }
    if (value instanceof BigDecimal bigDecimal) {
      return bigDecimal.toBigInteger();
    }
    return new BigInteger(value.toString());
  }
}
