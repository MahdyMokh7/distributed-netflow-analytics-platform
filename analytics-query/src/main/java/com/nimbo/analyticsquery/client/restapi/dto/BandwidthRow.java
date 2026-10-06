package com.nimbo.analyticsquery.client.restapi.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.math.BigInteger;
import java.time.Instant;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record BandwidthRow(
    String ip,
    Instant windowStart,
    Instant windowEnd,
    BigInteger inboundBytes,
    BigInteger outboundBytes,
    BigInteger totalBytes,
    BigInteger inboundPackets,
    BigInteger outboundPackets,
    BigInteger totalPackets,
    UUID batchId,
    Instant ingestedAt) {}
