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
public class TopIpAggregate {
  private String ip;
  private LocalDateTime windowStart;
  private LocalDateTime windowEnd;
  private long inboundBytes;
  private long outboundBytes;
  private long inboundPackets;
  private long outboundPackets;
  private UUID batchId;
  private LocalDateTime ingestedAt;
}
