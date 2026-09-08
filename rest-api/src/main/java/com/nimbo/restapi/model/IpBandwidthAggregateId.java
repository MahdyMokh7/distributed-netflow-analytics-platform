package com.nimbo.restapi.model;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class IpBandwidthAggregateId implements Serializable {
    private String ip;
    private Instant windowStart;
    private UUID batchId;
}
