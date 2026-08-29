package com.nimbo.restapi.repository.realtime;

import com.nimbo.restapi.client.StarRocksQueryClient;
import com.nimbo.restapi.config.StarRocksQueryProperties;
import com.nimbo.restapi.dto.response.realtime.RealtimeBandwidthResponseDto;
import com.nimbo.restapi.dto.response.realtime.RealtimeConnectionResponseDto;
import com.nimbo.restapi.dto.response.realtime.RealtimeProtocolResponseDto;
import com.nimbo.restapi.dto.response.realtime.RealtimeTrafficResponseDto;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

@Repository
public class RealtimeQueryRepository {

    private final NamedParameterJdbcTemplate jdbc;
    private final String table;

    public RealtimeQueryRepository(
            StarRocksQueryClient queryClient,
            StarRocksQueryProperties properties
    ) {
        this.jdbc = queryClient.jdbc();
        this.table = properties.qualifiedTable();
    }

    public RealtimeTrafficResponseDto findTrafficByIp(
            String ip,
            Instant startTime,
            Instant endTime
    ) {
        String sql = """
                SELECT
                    COALESCE(SUM(in_bytes), 0) AS in_bytes,
                    COALESCE(SUM(out_bytes), 0) AS out_bytes,
                    COALESCE(SUM(in_bytes + out_bytes), 0) AS total_bytes,

                    COALESCE(SUM(in_pkts), 0) AS in_packets,
                    COALESCE(SUM(out_pkts), 0) AS out_packets,
                    COALESCE(SUM(in_pkts + out_pkts), 0) AS total_packets,

                    COUNT(*) AS flow_count
                FROM %s
                WHERE kafka_timestamp >= :startTime
                  AND kafka_timestamp < :endTime
                  AND (
                      ipv4_src_addr = :ip
                      OR ipv4_dst_addr = :ip
                  )
                """.formatted(table);

        MapSqlParameterSource parameters =
                timeParameters(startTime, endTime)
                        .addValue("ip", ip);

        return jdbc.queryForObject(
                sql,
                parameters,
                (rs, rowNum) ->
                        RealtimeTrafficResponseDto.builder()
                                .ip(ip)
                                .startTime(startTime)
                                .endTime(endTime)
                                .inBytes(bigInteger(rs, "in_bytes"))
                                .outBytes(bigInteger(rs, "out_bytes"))
                                .totalBytes(bigInteger(rs, "total_bytes"))
                                .inPackets(bigInteger(rs, "in_packets"))
                                .outPackets(bigInteger(rs, "out_packets"))
                                .totalPackets(bigInteger(rs, "total_packets"))
                                .flowCount(rs.getLong("flow_count"))
                                .build()
        );
    }

    public Page<RealtimeConnectionResponseDto> findConnectionsByIp(
            String ip,
            Instant startTime,
            Instant endTime,
            int page,
            int size
    ) {
        String querySql = """
                SELECT
                    ipv4_src_addr,
                    ipv4_dst_addr,
                    l4_src_port,
                    l4_dst_port,
                    protocol,

                    in_bytes,
                    out_bytes,
                    in_pkts,
                    out_pkts,

                    kafka_timestamp,
                    kafka_partition,
                    kafka_offset
                FROM %s
                WHERE kafka_timestamp >= :startTime
                  AND kafka_timestamp < :endTime
                  AND (
                      ipv4_src_addr = :ip
                      OR ipv4_dst_addr = :ip
                  )
                ORDER BY
                    kafka_timestamp DESC,
                    kafka_partition DESC,
                    kafka_offset DESC
                LIMIT :limit
                OFFSET :offset
                """.formatted(table);

        String countSql = """
                SELECT COUNT(*)
                FROM %s
                WHERE kafka_timestamp >= :startTime
                  AND kafka_timestamp < :endTime
                  AND (
                      ipv4_src_addr = :ip
                      OR ipv4_dst_addr = :ip
                  )
                """.formatted(table);

        int offset = page * size;

        MapSqlParameterSource parameters =
                timeParameters(startTime, endTime)
                        .addValue("ip", ip)
                        .addValue("limit", size)
                        .addValue("offset", offset);

        List<RealtimeConnectionResponseDto> rows =
                jdbc.query(
                        querySql,
                        parameters,
                        (rs, rowNum) -> mapConnection(rs)
                );

        Long total =
                jdbc.queryForObject(
                        countSql,
                        timeParameters(startTime, endTime)
                                .addValue("ip", ip),
                        Long.class
                );

        return new PageImpl<>(
                rows,
                PageRequest.of(page, size),
                total == null ? 0 : total
        );
    }

    public List<RealtimeBandwidthResponseDto> findTopBandwidthIps(
            Instant startTime,
            Instant endTime,
            int limit
    ) {
        String sql = """
                SELECT
                    ip,
                    COALESCE(SUM(total_bytes), 0) AS total_bytes,
                    COALESCE(SUM(total_packets), 0) AS total_packets,
                    COUNT(*) AS flow_count
                FROM (
                    SELECT
                        ipv4_src_addr AS ip,
                        in_bytes + out_bytes AS total_bytes,
                        in_pkts + out_pkts AS total_packets
                    FROM %s
                    WHERE kafka_timestamp >= :startTime
                      AND kafka_timestamp < :endTime

                    UNION ALL

                    SELECT
                        ipv4_dst_addr AS ip,
                        in_bytes + out_bytes AS total_bytes,
                        in_pkts + out_pkts AS total_packets
                    FROM %s
                    WHERE kafka_timestamp >= :startTime
                      AND kafka_timestamp < :endTime
                      AND ipv4_dst_addr <> ipv4_src_addr
                ) endpoint_traffic
                GROUP BY ip
                ORDER BY total_bytes DESC
                LIMIT :limit
                """.formatted(table, table);

        MapSqlParameterSource parameters =
                timeParameters(startTime, endTime)
                        .addValue("limit", limit);

        return jdbc.query(
                sql,
                parameters,
                (rs, rowNum) ->
                        RealtimeBandwidthResponseDto.builder()
                                .ip(rs.getString("ip"))
                                .totalBytes(bigInteger(rs, "total_bytes"))
                                .totalPackets(bigInteger(rs, "total_packets"))
                                .flowCount(rs.getLong("flow_count"))
                                .build()
        );
    }

    public List<RealtimeProtocolResponseDto> findProtocolUsage(
            Instant startTime,
            Instant endTime
    ) {
        String sql = """
                SELECT
                    protocol,
                    COUNT(*) AS flow_count,

                    COALESCE(SUM(in_bytes), 0) AS in_bytes,
                    COALESCE(SUM(out_bytes), 0) AS out_bytes,
                    COALESCE(SUM(in_bytes + out_bytes), 0) AS total_bytes,

                    COALESCE(SUM(in_pkts), 0) AS in_packets,
                    COALESCE(SUM(out_pkts), 0) AS out_packets,
                    COALESCE(SUM(in_pkts + out_pkts), 0) AS total_packets

                FROM %s
                WHERE kafka_timestamp >= :startTime
                  AND kafka_timestamp < :endTime

                GROUP BY protocol
                ORDER BY flow_count DESC
                """.formatted(table);

        return jdbc.query(
                sql,
                timeParameters(startTime, endTime),
                (rs, rowNum) -> {
                    int protocol =
                            rs.getInt("protocol");

                    return RealtimeProtocolResponseDto.builder()
                            .protocol(protocol)
                            .protocolName(
                                    protocolName(protocol)
                            )
                            .flowCount(
                                    rs.getLong("flow_count")
                            )
                            .inBytes(
                                    bigInteger(rs, "in_bytes")
                            )
                            .outBytes(
                                    bigInteger(rs, "out_bytes")
                            )
                            .totalBytes(
                                    bigInteger(rs, "total_bytes")
                            )
                            .inPackets(
                                    bigInteger(rs, "in_packets")
                            )
                            .outPackets(
                                    bigInteger(rs, "out_packets")
                            )
                            .totalPackets(
                                    bigInteger(rs, "total_packets")
                            )
                            .build();
                }
        );
    }

    private RealtimeConnectionResponseDto mapConnection(
            ResultSet rs
    ) throws SQLException {

        int protocol =
                rs.getInt("protocol");

        BigInteger inBytes =
                bigInteger(rs, "in_bytes");

        BigInteger outBytes =
                bigInteger(rs, "out_bytes");

        BigInteger inPackets =
                bigInteger(rs, "in_pkts");

        BigInteger outPackets =
                bigInteger(rs, "out_pkts");

        return RealtimeConnectionResponseDto.builder()
                .srcIp(
                        rs.getString("ipv4_src_addr")
                )
                .dstIp(
                        rs.getString("ipv4_dst_addr")
                )
                .srcPort(
                        rs.getInt("l4_src_port")
                )
                .dstPort(
                        rs.getInt("l4_dst_port")
                )
                .protocol(protocol)
                .protocolName(
                        protocolName(protocol)
                )
                .inBytes(inBytes)
                .outBytes(outBytes)
                .totalBytes(
                        inBytes.add(outBytes)
                )
                .inPackets(inPackets)
                .outPackets(outPackets)
                .totalPackets(
                        inPackets.add(outPackets)
                )
                .timestamp(
                        Instant.ofEpochMilli(
                                rs.getLong(
                                        "kafka_timestamp"
                                )
                        )
                )
                .kafkaPartition(
                        rs.getInt(
                                "kafka_partition"
                        )
                )
                .kafkaOffset(
                        rs.getLong(
                                "kafka_offset"
                        )
                )
                .build();
    }

    private MapSqlParameterSource timeParameters(
            Instant startTime,
            Instant endTime
    ) {
        return new MapSqlParameterSource()
                .addValue(
                        "startTime",
                        startTime.toEpochMilli()
                )
                .addValue(
                        "endTime",
                        endTime.toEpochMilli()
                );
    }

    private BigInteger bigInteger(
            ResultSet resultSet,
            String column
    ) throws SQLException {

        Object value =
                resultSet.getObject(column);

        if (value == null) {
            return BigInteger.ZERO;
        }

        if (value instanceof BigInteger bigInteger) {
            return bigInteger;
        }

        if (value instanceof BigDecimal bigDecimal) {
            return bigDecimal.toBigInteger();
        }

        return new BigInteger(
                value.toString()
        );
    }

    private String protocolName(int protocol) {
        return switch (protocol) {
            case 1 -> "ICMP";
            case 6 -> "TCP";
            case 17 -> "UDP";
            default -> "PROTOCOL-" + protocol;
        };
    }
}