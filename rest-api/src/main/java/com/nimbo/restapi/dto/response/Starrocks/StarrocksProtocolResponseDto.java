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
public class StarrocksProtocolResponseDto {

  @Schema(example = "6.0")
  private Double l7Protocol;

  @Schema(example = "2026-09-21T08:00:00Z")
  private Instant windowStart;

  @Schema(example = "2026-09-21T09:00:00Z")
  private Instant windowEnd;

  @Schema(example = "1250000")
  private BigInteger inBytes;

  @Schema(example = "830000")
  private BigInteger outBytes;

  @Schema(example = "2080000")
  private BigInteger totalBytes;

  @Schema(example = "1250")
  private BigInteger inPackets;

  @Schema(example = "900")
  private BigInteger outPackets;

  @Schema(example = "2150")
  private BigInteger totalPackets;

  @Schema(example = "128")
  private Long connectionCount;
}
