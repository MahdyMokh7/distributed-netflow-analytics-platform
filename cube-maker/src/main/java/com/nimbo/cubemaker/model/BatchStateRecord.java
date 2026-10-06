package com.nimbo.cubemaker.model;

import java.time.Instant;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BatchStateRecord {
  private String fileName;
  private Instant fileTimestamp;
  private UUID batchId;
  private Instant processedAt;
}
