package com.nimbo.restapi.dto.response;

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
public class BandwidthResponseDto {

    private String ip;

    private Instant windowStart;

    private Instant windowEnd;

    private BigInteger inboundBytes;

    private BigInteger outboundBytes;

    private BigInteger totalBytes;

    private BigInteger inboundPackets;

    private BigInteger outboundPackets;

    private BigInteger totalPackets;

    private UUID batchId;

    private Instant ingestedAt;
}