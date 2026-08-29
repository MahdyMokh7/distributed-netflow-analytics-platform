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
public class RealtimeBandwidthResponseDto {

    private String ip;

    private BigInteger totalBytes;

    private BigInteger totalPackets;

    private long flowCount;
}