package com.nimbo.realtime.consumer;

import com.nimbo.realtime.avro.AvroDeserializer;
import com.nimbo.realtime.client.StarRocksStreamLoadClient;
import com.nimbo.realtime.config.StarRocksProperties;
import com.nimbo.realtime.mapper.StarRocksRecordMapper;
import com.nimbo.realtime.model.StarRocksRow;
import com.nimbo.realtime.model.StreamLoadResult;
import com.nimbo.realtime.validation.NetflowValidationService;
import com.nimbo.realtime.validation.ValidationResult;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
@RequiredArgsConstructor
public class StarRocksBatchProcessor {

    private static final String PUBLISH_TIMEOUT = "Publish Timeout";

    private final int workerNumber;
    private final StarRocksProperties starRocksProperties;
    private final AvroDeserializer avroDeserializer;
    private final NetflowValidationService validationService;
    private final StarRocksRecordMapper mapper;
    private final StarRocksStreamLoadClient streamLoadClient;

    private final List<StarRocksRow> rowBatch = new ArrayList<>();
    private final Map<TopicPartition, OffsetAndMetadata> pendingOffsets = new HashMap<>();
    private final Deque<PendingLoad> inFlightLoads = new ArrayDeque<>();
    private long lastFlushTime = System.currentTimeMillis();

    @Getter
    private boolean kafkaPaused;

    private record PendingLoad(
            String label,
            int rowCount,
            Map<TopicPartition, OffsetAndMetadata> offsets,
            CompletableFuture<StreamLoadResult> future
    ) {}

    public void process(ConsumerRecords<byte[], byte[]> records) {
        for (ConsumerRecord<byte[], byte[]> kafkaRecord : records) {
            processRecord(kafkaRecord);
        }
    }

    private void processRecord(ConsumerRecord<byte[], byte[]> kafkaRecord) {
        GenericRecord avroRecord = avroDeserializer.deserialize(kafkaRecord.value());
        ValidationResult validation = validationService.validate(avroRecord);
        markOffsetHandled(kafkaRecord);

        if (!validation.valid()) {
            return;
        }

        StarRocksRow row = mapper.map(
                avroRecord,
                kafkaRecord.topic(),
                kafkaRecord.partition(),
                kafkaRecord.offset(),
                kafkaRecord.timestamp()
        );
        rowBatch.add(row);
    }

    private void markOffsetHandled(ConsumerRecord<byte[], byte[]> kafkaRecord) {
        TopicPartition topicPartition = new TopicPartition(kafkaRecord.topic(), kafkaRecord.partition());
        pendingOffsets.put(topicPartition, new OffsetAndMetadata(kafkaRecord.offset() + 1));
    }

    public void flushIfNeeded() {
        if (!shouldFlush() || rowBatch.isEmpty()) return;

        submitAsync();
        rowBatch.clear();
        pendingOffsets.clear();
        lastFlushTime = System.currentTimeMillis();
    }

    private boolean shouldFlush() {
        return reachedSizeThreshold() || reachedTimeThreshold();
    }

    private boolean reachedSizeThreshold() {
        return rowBatch.size() >= starRocksProperties.getMaxBatchSizeRows();
    }

    private boolean reachedTimeThreshold() {
        if (rowBatch.isEmpty()) return false;
        long elapsed = System.currentTimeMillis() - lastFlushTime;
        return elapsed >= starRocksProperties.getMaxBatchTimeMs();
    }

    private void submitAsync() {
        List<StarRocksRow> rows = List.copyOf(rowBatch);
        Map<TopicPartition, OffsetAndMetadata> offsetsCopy = Map.copyOf(pendingOffsets);
        String label = generateDeterministicLabel(offsetsCopy);
        CompletableFuture<StreamLoadResult> future = loadWithRetryAsync(rows, label);

        inFlightLoads.addLast(new PendingLoad(label, rows.size(), offsetsCopy, future));
        log.debug("Worker {} submitted async Stream Load. label={}, rows={}, inFlight={}",
                workerNumber, label, rows.size(), inFlightLoads.size());
    }

    public void drainCompletedLoads(KafkaConsumer<byte[], byte[]> consumer) {
        while (hasCompletedHead()) {
            PendingLoad pending = inFlightLoads.peekFirst();
            StreamLoadResult result = awaitResult(pending);
            consumer.commitSync(pending.offsets());
            inFlightLoads.removeFirst();
            logCompletedLoad(pending, result);
        }
    }

    private boolean hasCompletedHead() {
        return !inFlightLoads.isEmpty() && inFlightLoads.peekFirst().future().isDone();
    }

    private StreamLoadResult awaitResult(PendingLoad pending) {
        try {
            return pending.future().join();
        } catch (CompletionException exception) {
            log.error("Async StarRocks load failed", exception.getCause());
            throw exception;
        }
    }

    private void logCompletedLoad(PendingLoad pending, StreamLoadResult result) {
        log.info("Worker {} completed Stream Load. label={}, rows={}, status={}, remainingInFlight={}",
                workerNumber, pending.label(), pending.rowCount(), result.status(),     inFlightLoads.size());
    }

    public boolean isBackpressured() {
        return inFlightLoads.size() >= starRocksProperties.getMaxInFlightRequests();
    }

    public void markKafkaPaused() {
        kafkaPaused = true;
    }

    public void markKafkaResumed() {
        kafkaPaused = false;
    }

    public void shutdown(KafkaConsumer<byte[], byte[]> consumer) {
        flushRemainingBatch();
        drainInFlightLoadsOnShutdown(consumer);
    }

    private void flushRemainingBatch() {
        if (rowBatch.isEmpty()) return;
        submitAsync();
        rowBatch.clear();
        pendingOffsets.clear();
    }

    private void drainInFlightLoadsOnShutdown(KafkaConsumer<byte[], byte[]> consumer) {
        while (!inFlightLoads.isEmpty()) {
            PendingLoad pending = inFlightLoads.peekFirst();
            try {
                pending.future().join();
                consumer.commitSync(pending.offsets());
                inFlightLoads.removeFirst();
            } catch (CompletionException exception) {
                log.error("Failed to complete Stream Load during shutdown", exception.getCause());
                return;
            }
        }
    }

    private CompletableFuture<StreamLoadResult> loadWithRetryAsync(List<StarRocksRow> rows, String label) {
        return streamLoadClient.loadAsync(rows, label)
                .handle((result, error) -> handleLoadResult(rows, label, result, error))
                .thenCompose(Function.identity());
    }

    private CompletableFuture<StreamLoadResult> handleLoadResult(List<StarRocksRow> rows
            , String label, StreamLoadResult result, Throwable error) {
        if (error == null && isSuccessfulLoad(result)) return CompletableFuture.completedFuture(result);
        logLoadFailure(result, error, label);
        return retryLater(rows, label);
    }

    private boolean isSuccessfulLoad(StreamLoadResult result) {
        return result.isSuccessful() || PUBLISH_TIMEOUT.equalsIgnoreCase(result.status());
    }

    private void logLoadFailure(StreamLoadResult result, Throwable error, String label) {
        if (error != null) {
            log.error("Worker {} Stream Load HTTP failure. label={}. Retrying...", workerNumber, label, error);
            return;
        }
        log.error("Worker {} Stream Load failed. status={}, message={}, label={}. Retrying...",
                workerNumber, result.status(), result.message(), label);
    }

    private CompletableFuture<StreamLoadResult> retryLater(List<StarRocksRow> rows, String label) {
        return delay(starRocksProperties.getRetryBackoffMs()).thenCompose(ignored -> loadWithRetryAsync(rows, label));
    }

    private CompletableFuture<Void> delay(long milliseconds) {
        return CompletableFuture.runAsync(() -> {}, CompletableFuture.delayedExecutor(milliseconds, TimeUnit.MILLISECONDS));
    }

    private String generateDeterministicLabel(Map<TopicPartition, OffsetAndMetadata> offsets) {
        StringBuilder builder = new StringBuilder("nimbo-w").append(workerNumber);
        offsets.entrySet().stream()
                .sorted(Comparator.comparingInt(entry -> entry.getKey().partition()))
                .forEach(entry -> appendOffsetToLabel(builder, entry));
        return builder.toString();
    }

    private void appendOffsetToLabel(StringBuilder builder, Map.Entry<TopicPartition, OffsetAndMetadata> entry) {
        builder.append("-p").append(entry.getKey().partition()).append("-o").append(entry.getValue().offset());
    }
}
