package com.nimbo.realtime.model;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;


public record PendingLoad(

        String label,
        int rowCount,
        Map<TopicPartition, OffsetAndMetadata> offsets,
        CompletableFuture<StreamLoadResult> future,
        long submittedNanos,
        AtomicLong completedNanos
) {
    public PendingLoad(String label, int rowCount, Map<TopicPartition, OffsetAndMetadata> offsets,
                       CompletableFuture<StreamLoadResult> future) {
        this(label, rowCount, offsets, future, System.nanoTime(), new AtomicLong());
    }

    public void markCompleted() {
        completedNanos.compareAndSet(0, System.nanoTime());
    }

    public long ageSeconds() {
        return Math.max(0, System.nanoTime() - submittedNanos) / 1_000_000_000;
    }

    public long drainDelayNanos() {
        long completion = completedNanos.get();
        return completion == 0 ? 0 : Math.max(0, System.nanoTime() - completion);
    }
}
