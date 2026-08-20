package com.nimbo.batch.model;

import java.time.LocalDateTime;

public class TrafficAggregate {

    private String ip;
    private String direction;
    private LocalDateTime hourTimestamp;
    private long totalBytes;
    private long totalPackets;
    private long recordCount;

    // Getters, Setters, Builder, toString
}