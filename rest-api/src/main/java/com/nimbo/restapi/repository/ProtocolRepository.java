package com.nimbo.restapi.repository;

import com.nimbo.restapi.model.ProtocolUsage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public interface ProtocolRepository extends JpaRepository<ProtocolUsage, Long> {

    @Query("SELECT p FROM ProtocolUsage p " +
            "WHERE p.windowStart >= :startTime " +
            "AND p.windowStart < :endTime " +
            "ORDER BY p.l7Protocol, p.windowStart DESC")
    List<ProtocolUsage> findByTimeRange(
            @Param("startTime") Instant startTime,
            @Param("endTime") Instant endTime
    );

    @Query("SELECT p FROM ProtocolUsage p " +
            "WHERE p.l7Protocol = :l7Protocol " +
            "AND p.windowStart >= :startTime " +
            "AND p.windowStart < :endTime " +
            "ORDER BY p.windowStart DESC")
    List<ProtocolUsage> findByProtocolAndTimeRange(
            @Param("l7Protocol") Double l7Protocol,
            @Param("startTime") Instant startTime,
            @Param("endTime") Instant endTime
    );
}