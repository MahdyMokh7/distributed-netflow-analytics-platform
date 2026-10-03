package com.nimbo.backfill.model;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

public record DailyBackfillPlan(String runId, BackfillMode mode, LocalDate day, Instant from, Instant to,
        String temporaryPartition, List<String> hdfsPaths) {

    public DailyBackfillPlan {
        hdfsPaths = List.copyOf(hdfsPaths);
        if (hdfsPaths.isEmpty()) {
            throw new IllegalArgumentException("hdfsPaths must not be empty");
        }
    }

    public long fromMillis() {
        return from.toEpochMilli();
    }

    public long toMillis() {
        return to.toEpochMilli();
    }

    public long fromMicros() {
        return Math.multiplyExact(fromMillis(), 1_000L);
    }

    public long toMicros() {
        return Math.multiplyExact(toMillis(), 1_000L);
    }

}
