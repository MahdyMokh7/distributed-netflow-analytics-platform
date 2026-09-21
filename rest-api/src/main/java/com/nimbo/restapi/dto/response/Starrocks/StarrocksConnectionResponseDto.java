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

  @Schema(example = "54321")
  private Integer srcPort;

  @Schema(example = "443")
  private Integer dstPort;

  @Schema(example = "6")
  private Integer protocol;

  @Schema(example = "TCP")
  private String protocolName;

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

  @Schema(example = "2026-09-21T08:42:15Z")
  private Instant timestamp;

  @Schema(example = "0")
  private Integer kafkaPartition;

  @Schema(example = "1042")
  private Long kafkaOffset;
}
