package com.nimbo.analyticsquery.client.restapi.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.math.BigInteger;
import java.time.Instant;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ProtocolRow(
    Double l7Protocol,
    Instant windowStart,
    Instant windowEnd,
    BigInteger inBytes,
    BigInteger outBytes,
    BigInteger totalBytes,
    BigInteger inPackets,
    BigInteger outPackets,
    BigInteger totalPackets,
    Long connectionCount,
    Instant ingestedAt) {}
