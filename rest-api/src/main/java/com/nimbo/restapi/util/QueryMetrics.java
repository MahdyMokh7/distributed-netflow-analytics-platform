package com.nimbo.restapi.util;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
public class QueryMetrics {

    private final MeterRegistry meterRegistry;

    public void recordQuery(String queryName, long durationMs) {
        meterRegistry
                .timer(
                        "nimbo.query.duration",
                        "query", queryName,
                        "status", "success"
                )
                .record(durationMs, TimeUnit.MILLISECONDS);
    }

    public void recordError(String queryName, long durationMs) {
        meterRegistry
                .counter(
                        "nimbo.query.errors",
                        "query", queryName
                )
                .increment();

        meterRegistry
                .timer(
                        "nimbo.query.duration",
                        "query", queryName,
                        "status", "error"
                )
                .record(durationMs, TimeUnit.MILLISECONDS);
    }
}