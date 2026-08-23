package com.nimbo.batch.model;

import java.time.LocalDateTime;

public class TrafficAggregate {
    private String srcIp;

    private LocalDateTime windowStart;
    private LocalDateTime windowEnd;

    private long inBytes;
    private long outBytes;

    private long inPackets;
    private long outPackets;

    // criteria for traffic
    private long recordCount;
    // Getters, Setters, Builder, toString
}