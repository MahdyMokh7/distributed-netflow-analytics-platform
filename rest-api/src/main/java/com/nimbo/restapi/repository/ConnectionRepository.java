package com.nimbo.restapi.repository;

import com.nimbo.restapi.model.ConnectionAggregate;
import com.nimbo.restapi.model.ConnectionAggregateId;
import com.nimbo.restapi.repository.projection.ConnectionWindowProjection;
import java.time.Instant;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface ConnectionRepository
    extends JpaRepository<ConnectionAggregate, ConnectionAggregateId> {

  String LOGICAL_WINDOW_COUNT =
      "SELECT COUNT(DISTINCT CONCAT(FUNCTION('encode', c.id, 'hex'), '|', c.srcIp, '|', "
          + "c.dstIp, '|', CAST(c.windowStart AS string), '|', "
          + "CAST(c.windowEnd AS string))) FROM ConnectionAggregate c ";

  @Query(
      value = "SELECT c.id AS id, c.srcIp AS srcIp, c.dstIp AS dstIp, "
          + "c.windowStart AS windowStart, c.windowEnd AS windowEnd, "
          + "SUM(c.inBytes) AS inBytes, SUM(c.outBytes) AS outBytes, "
          + "SUM(c.inPackets) AS inPackets, SUM(c.outPackets) AS outPackets, "
          + "MAX(c.ingestedAt) AS ingestedAt FROM ConnectionAggregate c "
          + "WHERE (c.srcIp = :ip OR c.dstIp = :ip) "
          + "AND c.windowStart >= :startTime "
          + "AND c.windowStart < :endTime "
          + "GROUP BY c.id, c.srcIp, c.dstIp, c.windowStart, c.windowEnd",
      countQuery = LOGICAL_WINDOW_COUNT
          + "WHERE (c.srcIp = :ip OR c.dstIp = :ip) "
          + "AND c.windowStart >= :startTime AND c.windowStart < :endTime")
  Page<ConnectionWindowProjection> findByIpAndTimeRange(
      @Param("ip") String ip,
      @Param("startTime") Instant startTime,
      @Param("endTime") Instant endTime,
      Pageable pageable);

  @Query(
      value = "SELECT c.id AS id, c.srcIp AS srcIp, c.dstIp AS dstIp, "
          + "c.windowStart AS windowStart, c.windowEnd AS windowEnd, "
          + "SUM(c.inBytes) AS inBytes, SUM(c.outBytes) AS outBytes, "
          + "SUM(c.inPackets) AS inPackets, SUM(c.outPackets) AS outPackets, "
          + "MAX(c.ingestedAt) AS ingestedAt FROM ConnectionAggregate c "
          + "WHERE c.srcIp = :srcIp "
          + "AND c.windowStart >= :startTime "
          + "AND c.windowStart < :endTime "
          + "GROUP BY c.id, c.srcIp, c.dstIp, c.windowStart, c.windowEnd",
      countQuery = LOGICAL_WINDOW_COUNT
          + "WHERE c.srcIp = :srcIp AND c.windowStart >= :startTime "
          + "AND c.windowStart < :endTime")
  Page<ConnectionWindowProjection> findBySrcIpAndTimeRange(
      @Param("srcIp") String srcIp,
      @Param("startTime") Instant startTime,
      @Param("endTime") Instant endTime,
      Pageable pageable);

  @Query(
      value = "SELECT c.id AS id, c.srcIp AS srcIp, c.dstIp AS dstIp, "
          + "c.windowStart AS windowStart, c.windowEnd AS windowEnd, "
          + "SUM(c.inBytes) AS inBytes, SUM(c.outBytes) AS outBytes, "
          + "SUM(c.inPackets) AS inPackets, SUM(c.outPackets) AS outPackets, "
          + "MAX(c.ingestedAt) AS ingestedAt FROM ConnectionAggregate c "
          + "WHERE c.dstIp = :dstIp "
          + "AND c.windowStart >= :startTime "
          + "AND c.windowStart < :endTime "
          + "GROUP BY c.id, c.srcIp, c.dstIp, c.windowStart, c.windowEnd",
      countQuery = LOGICAL_WINDOW_COUNT
          + "WHERE c.dstIp = :dstIp AND c.windowStart >= :startTime "
          + "AND c.windowStart < :endTime")
  Page<ConnectionWindowProjection> findByDstIpAndTimeRange(
      @Param("dstIp") String dstIp,
      @Param("startTime") Instant startTime,
      @Param("endTime") Instant endTime,
      Pageable pageable);

  @Query(
      value = "SELECT c.id AS id, c.srcIp AS srcIp, c.dstIp AS dstIp, "
          + "c.windowStart AS windowStart, c.windowEnd AS windowEnd, "
          + "SUM(c.inBytes) AS inBytes, SUM(c.outBytes) AS outBytes, "
          + "SUM(c.inPackets) AS inPackets, SUM(c.outPackets) AS outPackets, "
          + "MAX(c.ingestedAt) AS ingestedAt FROM ConnectionAggregate c "
          + "WHERE c.srcIp = :srcIp "
          + "AND c.dstIp = :dstIp "
          + "AND c.windowStart >= :startTime "
          + "AND c.windowStart < :endTime "
          + "GROUP BY c.id, c.srcIp, c.dstIp, c.windowStart, c.windowEnd",
      countQuery = LOGICAL_WINDOW_COUNT
          + "WHERE c.srcIp = :srcIp AND c.dstIp = :dstIp "
          + "AND c.windowStart >= :startTime AND c.windowStart < :endTime")
  Page<ConnectionWindowProjection> findBySrcIpAndDstIpAndTimeRange(
      @Param("srcIp") String srcIp,
      @Param("dstIp") String dstIp,
      @Param("startTime") Instant startTime,
      @Param("endTime") Instant endTime,
      Pageable pageable);

  @Query(
      value = "SELECT c.id AS id, c.srcIp AS srcIp, c.dstIp AS dstIp, "
          + "c.windowStart AS windowStart, c.windowEnd AS windowEnd, "
          + "SUM(c.inBytes) AS inBytes, SUM(c.outBytes) AS outBytes, "
          + "SUM(c.inPackets) AS inPackets, SUM(c.outPackets) AS outPackets, "
          + "MAX(c.ingestedAt) AS ingestedAt FROM ConnectionAggregate c "
          + "WHERE c.windowStart >= :startTime "
          + "AND c.windowStart < :endTime "
          + "GROUP BY c.id, c.srcIp, c.dstIp, c.windowStart, c.windowEnd",
      countQuery = LOGICAL_WINDOW_COUNT
          + "WHERE c.windowStart >= :startTime AND c.windowStart < :endTime")
  Page<ConnectionWindowProjection> findByTimeRange(
      @Param("startTime") Instant startTime, @Param("endTime") Instant endTime, Pageable pageable);
}
