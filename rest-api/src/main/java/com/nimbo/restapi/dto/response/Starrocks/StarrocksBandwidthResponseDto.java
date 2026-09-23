package com.nimbo.restapi.dto.response.Starrocks;

import io.swagger.v3.oas.annotations.media.Schema;
import java.math.BigInteger;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class StarrocksBandwidthResponseDto {

  @Schema(example = "192.0.2.10")
  private String ip;

  @Schema(example = "2080000")
  private BigInteger totalBytes;

  @Schema(example = "2150")
  private BigInteger totalPackets;

  @Schema(example = "42")
  private long flowCount;
}
