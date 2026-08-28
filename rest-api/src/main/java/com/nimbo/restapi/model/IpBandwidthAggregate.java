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
@Table(name = "ip_bandwidth_aggregates")
public class IpBandwidthAggregate {

    @Id
    private Long id;

    @Column(name = "ip", nullable = false)
    private String ip;

    @Column(name = "window_start", nullable = false)
    private Instant windowStart;

    @Column(name = "window_end", nullable = false)
    private Instant windowEnd;

    @Column(name = "inbound_bytes", nullable = false)
    private BigInteger inboundBytes;

    @Column(name = "outbound_bytes", nullable = false)
    private BigInteger outboundBytes;

    @Column(name = "inbound_packets", nullable = false)
    private BigInteger inboundPackets;

    @Column(name = "outbound_packets", nullable = false)
    private BigInteger outboundPackets;

    @Column(name = "batch_id", nullable = false)
    private UUID batchId;

    @Column(name = "ingested_at", nullable = false)
    private Instant ingestedAt;
}