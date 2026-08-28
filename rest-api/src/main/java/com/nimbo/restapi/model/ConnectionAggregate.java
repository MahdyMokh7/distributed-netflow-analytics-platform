package com.nimbo.restapi.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
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
@Table(name = "connection_aggregates")
public class ConnectionAggregate {

    @Id
    private String id;

    @Column(name = "src_ip", nullable = false)
    private String srcIp;

    @Column(name = "dst_ip", nullable = false)
    private String dstIp;

    @Column(name = "src_port", nullable = false)
    private Integer srcPort;

    @Column(name = "dst_port", nullable = false)
    private Integer dstPort;

    @Column(name = "l4_protocol", nullable = false)
    private Integer l4Protocol;

    @Transient
    private String protocolName;

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

    @Column(name = "batch_id", nullable = false)
    private UUID batchId;

    @Column(name = "ingested_at", nullable = false)
    private Instant ingestedAt;
}