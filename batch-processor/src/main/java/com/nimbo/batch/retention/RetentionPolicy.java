package com.nimbo.batch.retention;

import com.nimbo.batch.config.BatchConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.LocalDate;
import java.time.ZoneOffset;

@Component
public class RetentionPolicy {

    private final BatchConfig batchConfig;
    private final Clock clock;

    @Autowired
    public RetentionPolicy(BatchConfig batchConfig) {
        this(batchConfig, Clock.systemUTC());
    }

    RetentionPolicy(BatchConfig batchConfig, Clock clock) {
        this.batchConfig = batchConfig;
        this.clock = clock;
    }

    public RetentionWindow currentWindow() {
        if (!batchConfig.getRetention().isEnabled()) {
            return RetentionWindow.unbounded();
        }

        int previousDays = batchConfig.getRetention().getPreviousDays();
        if (previousDays < 0) {
            throw new IllegalStateException("batch.retention.previous-days must be nonnegative");
        }

        LocalDate cutoffDate = LocalDate.now(clock).minusDays(previousDays);
        return new RetentionWindow(cutoffDate, cutoffDate.atStartOfDay(ZoneOffset.UTC).toInstant());
    }
}
