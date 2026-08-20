package com.nimbo.batch.model;

import java.time.LocalDateTime;

public class ConnectionAggregate {

    private String srcIp;
    private String dstIp;
    private int srcPort;
    private int dstPort;
    private int protocol;
    private LocalDateTime windowStart;
    private LocalDateTime windowEnd;
    private long bytes;
    private long packets;

    // Getters, Setters, Builder, toString
}