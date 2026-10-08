package com.nimbo.analyticsquery.client.restapi.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.math.BigInteger;

@JsonIgnoreProperties(ignoreUnknown = true)
public record BandwidthRow(
    String ip,
    BigInteger inboundBytes,
    BigInteger outboundBytes,
    BigInteger totalBytes,
    BigInteger inboundPackets,
    BigInteger outboundPackets,
    BigInteger totalPackets) {}
