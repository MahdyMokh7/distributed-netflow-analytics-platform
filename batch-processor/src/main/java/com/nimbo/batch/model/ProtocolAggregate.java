package com.nimbo.batch.model;

import java.time.LocalDateTime;

public class ProtocolAggregate {

    private int protocol;
    private LocalDateTime hourTimestamp;
    private long totalBytes;
    private long totalPackets;
    private long connectionCount;

    // Getters, Setters, Builder, toString
}