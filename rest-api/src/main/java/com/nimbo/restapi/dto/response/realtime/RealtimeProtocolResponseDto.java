package com.nimbo.restapi.dto.response.realtime;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigInteger;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RealtimeProtocolResponseDto {

    private Integer protocol;

    private String protocolName;

    private long flowCount;

    private BigInteger inBytes;
    private BigInteger outBytes;
    private BigInteger totalBytes;

    private BigInteger inPackets;
    private BigInteger outPackets;
    private BigInteger totalPackets;
}