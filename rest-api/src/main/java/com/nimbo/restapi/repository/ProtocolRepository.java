package com.nimbo.restapi.repository;

import com.nimbo.restapi.model.ProtocolUsage;
import com.nimbo.restapi.model.ProtocolUsageId;
import com.nimbo.restapi.repository.projection.ProtocolWindowProjection;
import java.time.Instant;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface ProtocolRepository extends JpaRepository<ProtocolUsage, ProtocolUsageId> {

  @Query(
      "SELECT p.l7Protocol AS l7Protocol, p.windowStart AS windowStart, "
          + "p.windowEnd AS windowEnd, SUM(p.inBytes) AS inBytes, "
          + "SUM(p.outBytes) AS outBytes, SUM(p.inPackets) AS inPackets, "
          + "SUM(p.outPackets) AS outPackets, SUM(p.connectionCount) AS connectionCount, "
          + "MAX(p.ingestedAt) AS ingestedAt FROM ProtocolUsage p "
          + "WHERE p.windowStart >= :startTime "
          + "AND p.windowStart < :endTime "
          + "GROUP BY p.l7Protocol, p.windowStart, p.windowEnd "
          + "ORDER BY p.l7Protocol, p.windowStart DESC")
  List<ProtocolWindowProjection> findByTimeRange(
      @Param("startTime") Instant startTime, @Param("endTime") Instant endTime);

  @Query(
      "SELECT p.l7Protocol AS l7Protocol, p.windowStart AS windowStart, "
          + "p.windowEnd AS windowEnd, SUM(p.inBytes) AS inBytes, "
          + "SUM(p.outBytes) AS outBytes, SUM(p.inPackets) AS inPackets, "
          + "SUM(p.outPackets) AS outPackets, SUM(p.connectionCount) AS connectionCount, "
          + "MAX(p.ingestedAt) AS ingestedAt FROM ProtocolUsage p "
          + "WHERE p.l7Protocol = :l7Protocol "
          + "AND p.windowStart >= :startTime "
          + "AND p.windowStart < :endTime "
          + "GROUP BY p.l7Protocol, p.windowStart, p.windowEnd "
          + "ORDER BY p.windowStart DESC")
  List<ProtocolWindowProjection> findByProtocolAndTimeRange(
      @Param("l7Protocol") Double l7Protocol,
      @Param("startTime") Instant startTime,
      @Param("endTime") Instant endTime);
}
