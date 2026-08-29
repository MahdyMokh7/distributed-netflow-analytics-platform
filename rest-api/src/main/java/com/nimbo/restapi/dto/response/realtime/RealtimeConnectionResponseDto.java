package com.nimbo.restapi.dto.response.realtime;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigInteger;
import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RealtimeConnectionResponseDto {

    private String srcIp;
    private String dstIp;

    private Integer srcPort;
    private Integer dstPort;

    private Integer protocol;
    private String protocolName;

    private BigInteger inBytes;
    private BigInteger outBytes;
    private BigInteger totalBytes;

    private BigInteger inPackets;
    private BigInteger outPackets;
    private BigInteger totalPackets;

    private Instant timestamp;

    private Integer kafkaPartition;
    private Long kafkaOffset;
}