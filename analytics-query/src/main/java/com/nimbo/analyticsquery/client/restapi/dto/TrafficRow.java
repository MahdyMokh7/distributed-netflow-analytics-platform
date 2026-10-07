package com.nimbo.analyticsquery.client.restapi.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.math.BigInteger;
import java.time.Instant;

@JsonIgnoreProperties(ignoreUnknown = true)
public record TrafficRow(
    String srcIp,
    Instant windowStart,
    Instant windowEnd,
    BigInteger inBytes,
    BigInteger outBytes,
    BigInteger totalBytes,
    BigInteger inPackets,
    BigInteger outPackets,
    BigInteger totalPackets,
    BigInteger recordCount,
    Instant ingestedAt) {}
