package com.nimbo.analyticsquery.tool;

import com.nimbo.analyticsquery.intent.model.QueryRequirements;

import java.time.Duration;

final class RestToolCapabilities {

    private static final Duration MAX_TIME_RANGE = Duration.ofDays(7);

    private RestToolCapabilities() {
    }

    static boolean supportsTimeRange(
            QueryRequirements.TimeRange timeRange) {

        if (timeRange == null) {
            return false;
        }

        Duration duration = Duration.between(
                timeRange.start(),
                timeRange.end());

        return !duration.isZero()
                && !duration.isNegative()
                && duration.compareTo(MAX_TIME_RANGE) <= 0;
    }
}
