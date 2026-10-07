package com.nimbo.restapi.repository;

import com.nimbo.restapi.model.TrafficAggregate;
import com.nimbo.restapi.model.TrafficAggregateId;
import com.nimbo.restapi.repository.projection.TrafficWindowProjection;
import java.time.Instant;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface TrafficRepository extends JpaRepository<TrafficAggregate, TrafficAggregateId> {

  @Query(
      value =
          "SELECT t.srcIp AS srcIp, t.windowStart AS windowStart, "
              + "t.windowEnd AS windowEnd, SUM(t.inBytes) AS inBytes, "
              + "SUM(t.outBytes) AS outBytes, SUM(t.inPackets) AS inPackets, "
              + "SUM(t.outPackets) AS outPackets, SUM(t.recordCount) AS recordCount, "
              + "MAX(t.ingestedAt) AS ingestedAt FROM TrafficAggregate t "
              + "WHERE t.srcIp = :srcIp AND t.windowStart >= :startTime "
              + "AND t.windowStart < :endTime "
              + "GROUP BY t.srcIp, t.windowStart, t.windowEnd",
      countQuery =
          "SELECT COUNT(DISTINCT CONCAT(t.srcIp, '|', CAST(t.windowStart AS string), '|', "
              + "CAST(t.windowEnd AS string))) FROM TrafficAggregate t "
              + "WHERE t.srcIp = :srcIp AND t.windowStart >= :startTime "
              + "AND t.windowStart < :endTime")
  Page<TrafficWindowProjection> findBySrcIpAndTimeRange(
      @Param("srcIp") String srcIp,
      @Param("startTime") Instant startTime,
      @Param("endTime") Instant endTime,
      Pageable pageable);

  @Query(
      value =
          "SELECT t.srcIp AS srcIp, t.windowStart AS windowStart, "
              + "t.windowEnd AS windowEnd, SUM(t.inBytes) AS inBytes, "
              + "SUM(t.outBytes) AS outBytes, SUM(t.inPackets) AS inPackets, "
              + "SUM(t.outPackets) AS outPackets, SUM(t.recordCount) AS recordCount, "
              + "MAX(t.ingestedAt) AS ingestedAt FROM TrafficAggregate t "
              + "WHERE t.windowStart >= :startTime AND t.windowStart < :endTime "
              + "GROUP BY t.srcIp, t.windowStart, t.windowEnd",
      countQuery =
          "SELECT COUNT(DISTINCT CONCAT(t.srcIp, '|', CAST(t.windowStart AS string), '|', "
              + "CAST(t.windowEnd AS string))) FROM TrafficAggregate t "
              + "WHERE t.windowStart >= :startTime AND t.windowStart < :endTime")
  Page<TrafficWindowProjection> findByTimeRange(
      @Param("startTime") Instant startTime, @Param("endTime") Instant endTime, Pageable pageable);

}
