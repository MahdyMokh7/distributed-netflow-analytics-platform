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
public class RealtimeTrafficResponseDto {

    private String ip;

    private Instant startTime;
    private Instant endTime;

    private BigInteger inBytes;
    private BigInteger outBytes;
    private BigInteger totalBytes;

    private BigInteger inPackets;
    private BigInteger outPackets;
    private BigInteger totalPackets;

    private long flowCount;
}