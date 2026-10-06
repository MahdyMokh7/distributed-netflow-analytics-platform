package com.nimbo.cubemaker.model;

import java.time.LocalDateTime;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TrafficAggregate {
  private String srcIp;
  private LocalDateTime windowStart;
  private LocalDateTime windowEnd;
  private long inBytes;
  private long outBytes;
  private long inPackets;
  private long outPackets;
  private long recordCount;
  private UUID batchId;
  private LocalDateTime ingestedAt;
}
