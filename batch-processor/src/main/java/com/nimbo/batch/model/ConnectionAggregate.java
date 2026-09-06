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
public class ConnectionAggregate {
    private String id;
    private String srcIp;
    private String dstIp;
    private LocalDateTime windowStart;
    private LocalDateTime windowEnd;
    private long inBytes;
    private long outBytes;
    private long inPackets;
    private long outPackets;
    private UUID batchId;
    private LocalDateTime ingestedAt;
}
