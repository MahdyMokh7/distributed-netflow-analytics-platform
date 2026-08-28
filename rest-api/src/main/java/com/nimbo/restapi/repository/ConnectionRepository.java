package com.nimbo.restapi.repository;

import com.nimbo.restapi.model.ConnectionAggregate;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;

@Repository
public interface ConnectionRepository extends JpaRepository<ConnectionAggregate, String> {

    @Query("SELECT c FROM ConnectionAggregate c " +
            "WHERE (c.srcIp = :ip OR c.dstIp = :ip) " +
            "AND c.windowStart >= :startTime " +
            "AND c.windowStart < :endTime " +
            "ORDER BY c.windowStart DESC")
    Page<ConnectionAggregate> findByIpAndTimeRange(
            @Param("ip") String ip,
            @Param("startTime") Instant startTime,
            @Param("endTime") Instant endTime,
            Pageable pageable
    );

    @Query("SELECT c FROM ConnectionAggregate c " +
            "WHERE c.srcIp = :srcIp " +
            "AND c.windowStart >= :startTime " +
            "AND c.windowStart < :endTime " +
            "ORDER BY c.windowStart DESC")
    Page<ConnectionAggregate> findBySrcIpAndTimeRange(
            @Param("srcIp") String srcIp,
            @Param("startTime") Instant startTime,
            @Param("endTime") Instant endTime,
            Pageable pageable
    );

    @Query("SELECT c FROM ConnectionAggregate c " +
            "WHERE c.dstIp = :dstIp " +
            "AND c.windowStart >= :startTime " +
            "AND c.windowStart < :endTime " +
            "ORDER BY c.windowStart DESC")
    Page<ConnectionAggregate> findByDstIpAndTimeRange(
            @Param("dstIp") String dstIp,
            @Param("startTime") Instant startTime,
            @Param("endTime") Instant endTime,
            Pageable pageable
    );

    @Query("SELECT c FROM ConnectionAggregate c " +
            "WHERE c.srcIp = :srcIp " +
            "AND c.dstIp = :dstIp " +
            "AND c.windowStart >= :startTime " +
            "AND c.windowStart < :endTime " +
            "ORDER BY c.windowStart DESC")
    Page<ConnectionAggregate> findBySrcIpAndDstIpAndTimeRange(
            @Param("srcIp") String srcIp,
            @Param("dstIp") String dstIp,
            @Param("startTime") Instant startTime,
            @Param("endTime") Instant endTime,
            Pageable pageable
    );

    @Query("SELECT c FROM ConnectionAggregate c " +
            "WHERE c.windowStart >= :startTime " +
            "AND c.windowStart < :endTime " +
            "ORDER BY c.windowStart DESC")
    Page<ConnectionAggregate> findByTimeRange(
            @Param("startTime") Instant startTime,
            @Param("endTime") Instant endTime,
            Pageable pageable
    );
}