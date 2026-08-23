package com.nimbo.batch.model;

import java.time.LocalDateTime;

public class ConnectionAggregate {
    // sha2 hash of (srcIp, dstIp, srcPort, dstPort, l4Protocol, windowStart)
    private String id;

    // connection identity: (srcIp, dstIp, srcPort, dstPort, l4Protocol)
    private String srcIp;
    private String dstIp;
    private int srcPort;
    private int dstPort;
    private int l4Protocol;

    private LocalDateTime windowStart;
    private LocalDateTime windowEnd;

    private long inBytes;
    private long outBytes;

    private long inPackets;
    private long outPackets;
    // Getters, Setters, Builder, toString
}