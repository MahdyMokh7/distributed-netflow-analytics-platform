package com.nimbo.realtime.model;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

class PendingLoadTest {

    @Test
    void shouldCreatePendingLoad() {
        CompletableFuture<StreamLoadResult> future = CompletableFuture.completedFuture(null);

        Map<TopicPartition, OffsetAndMetadata> offsets = Map.of(
                new TopicPartition("topic", 0),
                new OffsetAndMetadata(10)
        );

        PendingLoad load = new PendingLoad("label-1", 100, offsets, future);

        assertThat(load.label()).isEqualTo("label-1");
        assertThat(load.rowCount()).isEqualTo(100);
        assertThat(load.offsets()).containsEntry(
                new TopicPartition("topic", 0),
                new OffsetAndMetadata(10)
        );
        assertThat(load.future()).isSameAs(future);
        assertThat(load.ageSeconds()).isGreaterThanOrEqualTo(0);
    }

    @Test
    void shouldStoreCompletedFuture() {
        StreamLoadResult result = null;
        CompletableFuture<StreamLoadResult> future = CompletableFuture.completedFuture(result);

        PendingLoad load = new PendingLoad("label", 1, Map.of(), future);

        assertThat(load.future().isDone()).isTrue();

        load.markCompleted();
        assertThat(load.drainDelayNanos()).isGreaterThanOrEqualTo(0);
    }
}
