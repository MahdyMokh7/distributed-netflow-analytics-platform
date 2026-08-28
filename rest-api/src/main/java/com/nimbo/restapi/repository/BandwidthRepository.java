package com.nimbo.restapi.repository;

import com.nimbo.restapi.model.IpBandwidthAggregate;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public interface BandwidthRepository extends JpaRepository<IpBandwidthAggregate, Long> {

    @Query("SELECT b FROM IpBandwidthAggregate b " +
            "WHERE b.ip = :ip " +
            "AND b.windowStart >= :startTime " +
            "AND b.windowStart < :endTime " +
            "ORDER BY b.windowStart DESC")
    List<IpBandwidthAggregate> findByIpAndTimeRange(
            @Param("ip") String ip,
            @Param("startTime") Instant startTime,
            @Param("endTime") Instant endTime
    );

    @Query("SELECT b.ip, " +
            "SUM(b.inboundBytes) as totalInboundBytes, " +
            "SUM(b.outboundBytes) as totalOutboundBytes, " +
            "SUM(b.inboundPackets) as totalInboundPackets, " +
            "SUM(b.outboundPackets) as totalOutboundPackets, " +
            "SUM(b.inboundBytes + b.outboundBytes) as totalBytes " +
            "FROM IpBandwidthAggregate b " +
            "WHERE b.windowStart >= :startTime " +
            "AND b.windowStart < :endTime " +
            "GROUP BY b.ip " +
            "ORDER BY totalBytes DESC")
    Page<Object[]> findTopBandwidthIps(
            @Param("startTime") Instant startTime,
            @Param("endTime") Instant endTime,
            Pageable pageable
    );
}