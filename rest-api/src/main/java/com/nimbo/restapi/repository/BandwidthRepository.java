package com.nimbo.restapi.repository;

import com.nimbo.restapi.model.IpBandwidthAggregate;
import com.nimbo.restapi.model.IpBandwidthAggregateId;
import com.nimbo.restapi.repository.projection.BandwidthSummaryProjection;
import com.nimbo.restapi.repository.projection.BandwidthWindowProjection;
import java.time.Instant;
import java.util.List;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface BandwidthRepository
    extends JpaRepository<IpBandwidthAggregate, IpBandwidthAggregateId> {

  @Query(
      "SELECT b.ip AS ip, b.windowStart AS windowStart, b.windowEnd AS windowEnd, "
          + "SUM(b.inboundBytes) AS inboundBytes, SUM(b.outboundBytes) AS outboundBytes, "
          + "SUM(b.inboundPackets) AS inboundPackets, "
          + "SUM(b.outboundPackets) AS outboundPackets, MAX(b.ingestedAt) AS ingestedAt "
          + "FROM IpBandwidthAggregate b "
          + "WHERE b.ip = :ip "
          + "AND b.windowStart >= :startTime "
          + "AND b.windowStart < :endTime "
          + "GROUP BY b.ip, b.windowStart, b.windowEnd "
          + "ORDER BY b.windowStart DESC")
  List<BandwidthWindowProjection> findByIpAndTimeRange(
      @Param("ip") String ip,
      @Param("startTime") Instant startTime,
      @Param("endTime") Instant endTime);

  @Query(
      "SELECT b.ip AS ip, "
          + "SUM(b.inboundBytes) AS inboundBytes, "
          + "SUM(b.outboundBytes) AS outboundBytes, "
          + "SUM(b.inboundPackets) AS inboundPackets, "
          + "SUM(b.outboundPackets) AS outboundPackets, "
          + "SUM(b.inboundBytes + b.outboundBytes) AS totalBytes "
          + "FROM IpBandwidthAggregate b "
          + "WHERE b.windowStart >= :startTime "
          + "AND b.windowStart < :endTime "
          + "GROUP BY b.ip "
          + "ORDER BY totalBytes DESC")
  List<BandwidthSummaryProjection> findTopBandwidthIps(
      @Param("startTime") Instant startTime, @Param("endTime") Instant endTime, Pageable pageable);
}
