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

  @Schema(example = "1250000")
  private BigInteger inboundBytes;

  @Schema(example = "830000")
  private BigInteger outboundBytes;

  @Schema(example = "2080000")
  private BigInteger totalBytes;

  @Schema(example = "1250")
  private BigInteger inboundPackets;

  @Schema(example = "900")
  private BigInteger outboundPackets;

  @Schema(example = "2150")
  private BigInteger totalPackets;

}
