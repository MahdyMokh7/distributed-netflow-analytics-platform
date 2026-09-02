package com.nimbo.realtime.model;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.concurrent.CompletableFuture;


public record PendingLoad(

        String label,
        int rowCount,
        Map<TopicPartition, OffsetAndMetadata> offsets,
        CompletableFuture<StreamLoadResult> future
) {}
