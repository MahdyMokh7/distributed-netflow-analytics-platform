package com.nimbo.restapi.dto.response;

import java.math.BigInteger;
import java.time.Instant;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TrafficResponseDto {

  private String srcIp;

  private Instant windowStart;

  private Instant windowEnd;

  private BigInteger inBytes;

  private BigInteger outBytes;

  private BigInteger totalBytes;

  private BigInteger inPackets;

  private BigInteger outPackets;

  private BigInteger totalPackets;

  private BigInteger recordCount;

  private UUID batchId;

  private Instant ingestedAt;
}
