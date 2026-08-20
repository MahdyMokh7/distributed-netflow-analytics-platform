package com.nimbo.batch.model;

import java.time.LocalDate;

public class TopIpAggregate {

    private String ip;
    private LocalDate date;
    private long inboundBytes;
    private long outboundBytes;
    private long totalBytes;
    private Integer rank;

    // Getters, Setters, Builder, toString

    public long getTotalBytes() {
        return inboundBytes + outboundBytes;
    }
}