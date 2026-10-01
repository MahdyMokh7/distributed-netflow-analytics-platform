package com.nimbo.analyticsquery.client.restapi.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigInteger;
import java.time.Instant;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ConnectionRow(
        String id,
        String srcIp,
        String dstIp,
        Instant windowStart,
        Instant windowEnd,
        BigInteger inBytes,
        BigInteger outBytes,
        BigInteger totalBytes,
        BigInteger inPackets,
        BigInteger outPackets,
        BigInteger totalPackets,
        UUID batchId,
        Instant ingestedAt) {
}