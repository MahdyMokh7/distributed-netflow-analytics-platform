package com.nimbo.restapi.dto.response;

import java.math.BigInteger;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TopBandwidthResponseDto {

  private String ip;

  private BigInteger inboundBytes;

  private BigInteger outboundBytes;

  private BigInteger totalBytes;

  private BigInteger inboundPackets;

  private BigInteger outboundPackets;

  private BigInteger totalPackets;
}
