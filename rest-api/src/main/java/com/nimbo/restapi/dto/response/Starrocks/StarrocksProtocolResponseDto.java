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
public class StarrocksProtocolResponseDto {

  @Schema(example = "6")
  private Integer protocol;

  @Schema(example = "TCP")
  private String protocolName;

  @Schema(example = "128")
  private long flowCount;

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
}
