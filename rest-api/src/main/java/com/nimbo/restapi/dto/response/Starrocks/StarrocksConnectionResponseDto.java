package com.nimbo.restapi.dto.response.Starrocks;

import io.swagger.v3.oas.annotations.media.Schema;
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
public class StarrocksConnectionResponseDto {

  @Schema(example = "192.0.2.10")
  private String srcIp;

  @Schema(example = "198.51.100.20")
  private String dstIp;

  @Schema(example = "2026-09-21T08:00:00Z")
  private Instant windowStart;

  @Schema(example = "2026-09-21T09:00:00Z")
  private Instant windowEnd;

  @Schema(example = "125000")
  private BigInteger inBytes;

  @Schema(example = "83000")
  private BigInteger outBytes;

  @Schema(example = "208000")
  private BigInteger totalBytes;

  @Schema(example = "125")
  private BigInteger inPackets;

  @Schema(example = "90")
  private BigInteger outPackets;

  @Schema(example = "215")
  private BigInteger totalPackets;

}
