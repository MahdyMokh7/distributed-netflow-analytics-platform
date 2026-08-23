package com.nimbo.batch.model;

import java.time.LocalDateTime;

public class TopIpAggregate {
    // unified: appears as either src or dst
    private String ip;

    private LocalDateTime windowStart;
    private LocalDateTime windowEnd;

    private long inboundBytes;
    private long outboundBytes;

    private long inboundPackets;
    private long outboundPackets;

    // Getters, Setters, Builder, toString
}