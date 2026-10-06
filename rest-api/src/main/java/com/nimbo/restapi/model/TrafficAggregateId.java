package com.nimbo.restapi.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class TrafficAggregateId implements Serializable {
  private String srcIp;
  private Instant windowStart;
  private UUID batchId;
}
