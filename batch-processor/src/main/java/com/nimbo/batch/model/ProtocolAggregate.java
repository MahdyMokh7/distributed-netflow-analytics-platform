package com.nimbo.batch.model;

import java.time.LocalDateTime;

public class ProtocolAggregate {
    private double l7Protocol;

    private LocalDateTime windowStart;
    private LocalDateTime windowEnd;

    private long inBytes;
    private long outBytes;

    private long inPackets;
    private long outPackets;

    private long connectionCount;
    // Getters, Setters, Builder, toString
}