package com.nimbo.batch.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

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