package com.nimbo.restapi.dto.response;

import java.math.BigInteger;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConnectionResponseDto {

  private String id;

  private String srcIp;

  private String dstIp;

  private Instant windowStart;

  private Instant windowEnd;

  private BigInteger inBytes;

  private BigInteger outBytes;

  private BigInteger totalBytes;

  private BigInteger inPackets;

  private BigInteger outPackets;

  private BigInteger totalPackets;

  private Instant ingestedAt;
}
