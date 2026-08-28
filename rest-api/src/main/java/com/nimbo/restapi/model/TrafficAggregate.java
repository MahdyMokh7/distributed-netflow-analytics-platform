package com.nimbo.restapi.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigInteger;
import java.time.Instant;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "traffic_aggregates")
public class TrafficAggregate {

    @Id
    private Long id;

    @Column(name = "src_ip", nullable = false)
    private String srcIp;

    @Column(name = "window_start", nullable = false)
    private Instant windowStart;

    @Column(name = "window_end", nullable = false)
    private Instant windowEnd;

    @Column(name = "in_bytes", nullable = false)
    private BigInteger inBytes;

    @Column(name = "out_bytes", nullable = false)
    private BigInteger outBytes;

    @Column(name = "in_packets", nullable = false)
    private BigInteger inPackets;

    @Column(name = "out_packets", nullable = false)
    private BigInteger outPackets;

    @Column(name = "record_count", nullable = false)
    private BigInteger recordCount;

    @Column(name = "batch_id", nullable = false)
    private UUID batchId;

    @Column(name = "ingested_at", nullable = false)
    private Instant ingestedAt;
}