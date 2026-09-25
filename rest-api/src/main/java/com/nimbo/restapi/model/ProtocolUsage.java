package com.nimbo.restapi.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.math.BigInteger;
import java.time.Instant;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "protocol_usage_aggregates")
@IdClass(ProtocolUsageId.class)
public class ProtocolUsage {

    @Id
    @Column(name = "l7_protocol", nullable = false)
    private Double l7Protocol;

    @Id
    @Column(name = "window_start", nullable = false)
    private Instant windowStart;

    @Column(name = "window_end", nullable = false)
    private Instant windowEnd;

    @Column(name = "in_bytes", nullable = false, columnDefinition = "BIGINT")
    @JdbcTypeCode(SqlTypes.BIGINT)
    private BigInteger inBytes;

    @Column(name = "out_bytes", nullable = false, columnDefinition = "BIGINT")
    @JdbcTypeCode(SqlTypes.BIGINT)
    private BigInteger outBytes;

    @Column(name = "in_packets", nullable = false, columnDefinition = "BIGINT")
    @JdbcTypeCode(SqlTypes.BIGINT)
    private BigInteger inPackets;

    @Column(name = "out_packets", nullable = false, columnDefinition = "BIGINT")
    @JdbcTypeCode(SqlTypes.BIGINT)
    private BigInteger outPackets;

    @Column(name = "connection_count", nullable = false)
    private Long connectionCount;

    @Id
    @Column(name = "batch_id", nullable = false)
    private UUID batchId;

    @Column(name = "ingested_at", nullable = false)
    private Instant ingestedAt;
}
