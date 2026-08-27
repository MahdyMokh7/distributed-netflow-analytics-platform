package com.nimbo.batch.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

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