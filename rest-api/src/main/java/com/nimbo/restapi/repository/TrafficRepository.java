package com.nimbo.restapi.repository;

import com.nimbo.restapi.model.TrafficAggregate;
import com.nimbo.restapi.model.TrafficAggregateId;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;

@Repository
public interface TrafficRepository extends JpaRepository<TrafficAggregate, TrafficAggregateId> {

    @Query("SELECT t FROM TrafficAggregate t " +
            "WHERE t.srcIp = :srcIp " +
            "AND t.windowStart >= :startTime " +
            "AND t.windowStart < :endTime")
    Page<TrafficAggregate> findBySrcIpAndTimeRange(
            @Param("srcIp") String srcIp,
            @Param("startTime") Instant startTime,
            @Param("endTime") Instant endTime,
            Pageable pageable
    );

    @Query("SELECT t FROM TrafficAggregate t " +
            "WHERE t.windowStart >= :startTime " +
            "AND t.windowStart < :endTime")
    Page<TrafficAggregate> findByTimeRange(
            @Param("startTime") Instant startTime,
            @Param("endTime") Instant endTime,
            Pageable pageable
    );

    @Query("SELECT t.srcIp, " +
            "SUM(t.inBytes) as totalInBytes, " +
            "SUM(t.outBytes) as totalOutBytes, " +
            "SUM(t.inBytes + t.outBytes) as totalBytes " +
            "FROM TrafficAggregate t " +
            "WHERE t.windowStart >= :startTime " +
            "AND t.windowStart < :endTime " +
            "GROUP BY t.srcIp " +
            "ORDER BY totalBytes DESC")
    Page<Object[]> findTopTrafficIps(
            @Param("startTime") Instant startTime,
            @Param("endTime") Instant endTime,
            Pageable pageable
    );
}
