package com.nimbo.storage.service.batching;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

public record BatchPolicy(long maxRecordCount, long maxSizeBytes, Duration maxBatchAge) {

    public BatchPolicy(
            long maxRecordCount,
            long maxSizeBytes,
            Duration maxBatchAge
    ) {
        if (maxRecordCount <= 0) {
            throw new IllegalArgumentException(
                    "maxRecordCount must be positive"
            );
        }

        if (maxSizeBytes <= 0) {
            throw new IllegalArgumentException(
                    "maxSizeBytes must be positive"
            );
        }

        this.maxBatchAge = Objects.requireNonNull(
                maxBatchAge,
                "maxBatchAge must not be null"
        );

        if (maxBatchAge.isZero() || maxBatchAge.isNegative()) {
            throw new IllegalArgumentException(
                    "maxBatchAge must be positive"
            );
        }

        this.maxRecordCount = maxRecordCount;
        this.maxSizeBytes = maxSizeBytes;
    }

    public boolean shouldFlush(
            long recordCount,
            long totalSizeBytes,
            Instant firstRecordAt,
            Instant now
    ) {
        Objects.requireNonNull(now, "now must not be null");

        if (recordCount == 0) {
            return false;
        }

        if (recordCount >= maxRecordCount) {
            return true;
        }

        if (totalSizeBytes >= maxSizeBytes) {
            return true;
        }

        if (firstRecordAt == null) {
            return false;
        }

        return Duration.between(firstRecordAt, now)
                .compareTo(maxBatchAge) >= 0;
    }
}