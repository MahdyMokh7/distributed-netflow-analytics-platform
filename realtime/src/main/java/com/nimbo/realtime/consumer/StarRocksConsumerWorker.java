package com.nimbo.realtime.consumer;

import com.nimbo.realtime.avro.AvroDeserializer;
import com.nimbo.realtime.client.StarRocksStreamLoadClient;
import com.nimbo.realtime.config.KafkaProperties;
import com.nimbo.realtime.config.StarRocksProperties;
import com.nimbo.realtime.mapper.StarRocksRecordMapper;
import com.nimbo.realtime.model.StarRocksRow;
import com.nimbo.realtime.model.StreamLoadResult;
import com.nimbo.realtime.validation.NetflowValidationService;
import com.nimbo.realtime.validation.ValidationResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
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
public class StarRocksConsumerWorker implements Runnable {

    private static final long BACKPRESSURE_POLL_MS = 100L;
    private static final String PUBLISH_TIMEOUT = "Publish Timeout";
    private final int workerNumber;
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final KafkaProperties kafkaProperties;
    private final StarRocksProperties starRocksProperties;
    private final AvroDeserializer avroDeserializer;
    private final NetflowValidationService validationService;
    private final StarRocksRecordMapper mapper;
    private final StarRocksStreamLoadClient streamLoadClient;
    private volatile boolean running = true;
    private record PendingLoad(String label, int rowCount, Map<TopicPartition, OffsetAndMetadata> offsets,
                               CompletableFuture<StreamLoadResult> future) {}

    private static final class WorkerState {
        private final List<StarRocksRow> rowBatch = new ArrayList<>();
        private final Map<TopicPartition, OffsetAndMetadata> pendingOffsets = new HashMap<>();
        private final Deque<PendingLoad> inFlightLoads = new ArrayDeque<>();
        private long lastFlushTime = System.currentTimeMillis();
        private boolean kafkaPaused;
    }

    @Override
    public void run() {
        WorkerState state = new WorkerState();
        try {
            startConsumer();
            while (running) {
                processIteration(state);
            }
        } catch (WakeupException exception) {
            handleWakeup(exception);
        } catch (Exception exception) {
            log.error("Worker {} stopped due to unexpected error", workerNumber, exception);
        } finally {
            closeConsumer();
        }
    }

    private void processIteration(WorkerState state) {
        drainCompletedLoads(state.inFlightLoads);
        if (isBackpressured(state)) {
            handleBackpressure(state);
            return;
        }
        resumeKafkaIfNeeded(state);
        ConsumerRecords<byte[], byte[]> records = pollRecords();
        processRecords(records, state);
        flushIfNeeded(state);
    }

    private void startConsumer() {
        consumer.subscribe(List.of(kafkaProperties.getTopic()));
        log.info("StarRocks consumer worker {} started", workerNumber);
    }

    private ConsumerRecords<byte[], byte[]> pollRecords() {
        return consumer.poll(Duration.ofMillis(kafkaProperties.getPollTimeoutMs()));
    }

    private void processRecords(ConsumerRecords<byte[], byte[]> records, WorkerState state) {
        for (ConsumerRecord<byte[], byte[]> kafkaRecord : records) {
            processRecord(kafkaRecord, state);
        }
    }

    private void processRecord(ConsumerRecord<byte[], byte[]> kafkaRecord, WorkerState state) {
        GenericRecord avroRecord = avroDeserializer.deserialize(kafkaRecord.value());
        ValidationResult validation = validationService.validate(avroRecord);
        markOffsetHandled(kafkaRecord, state);

        if (!validation.valid()) {
            logInvalidRecord(kafkaRecord, validation);
            return;
        }

        StarRocksRow row = mapper.map(avroRecord, kafkaRecord.topic(), kafkaRecord.partition(),
                kafkaRecord.offset(), kafkaRecord.timestamp());
        state.rowBatch.add(row);
    }

    private void markOffsetHandled(ConsumerRecord<byte[], byte[]> kafkaRecord, WorkerState state) {
        TopicPartition topicPartition = new TopicPartition(kafkaRecord.topic(), kafkaRecord.partition());
        state.pendingOffsets.put(topicPartition, new OffsetAndMetadata(kafkaRecord.offset() + 1));
    }

    private void logInvalidRecord(ConsumerRecord<byte[], byte[]> kafkaRecord, ValidationResult validation) {
        log.warn("Skipping invalid record. topic={}, partition={}, offset={}, reason={}",
                kafkaRecord.topic(), kafkaRecord.partition(), kafkaRecord.offset(), validation.reason());
    }

    private boolean isBackpressured(WorkerState state) {
        return state.inFlightLoads.size() >= starRocksProperties.getMaxInFlightRequests();
    }

    private void handleBackpressure(WorkerState state) {
        pauseKafkaIfNeeded(state);
        consumer.poll(Duration.ofMillis(BACKPRESSURE_POLL_MS));
    }

    private void pauseKafkaIfNeeded(WorkerState state) {
        if (state.kafkaPaused || consumer.assignment().isEmpty()) return;
        consumer.pause(consumer.assignment());
        state.kafkaPaused = true;
        log.debug("Worker {} paused Kafka. inFlight={}", workerNumber, state.inFlightLoads.size());
    }

    private void resumeKafkaIfNeeded(WorkerState state) {
        if (!state.kafkaPaused) return;
        consumer.resume(consumer.assignment());
        state.kafkaPaused = false;
        log.debug("Worker {} resumed Kafka. inFlight={}", workerNumber, state.inFlightLoads.size());
    }

    private void flushIfNeeded(WorkerState state) {
        if (!shouldFlush(state)) return;
        submitAsync(state.rowBatch, state.pendingOffsets, state.inFlightLoads);
        state.rowBatch.clear();
        state.pendingOffsets.clear();
        state.lastFlushTime = System.currentTimeMillis();
    }

    private boolean shouldFlush(WorkerState state) {
        return reachedSizeThreshold(state) || reachedTimeThreshold(state);
    }

    private boolean reachedSizeThreshold(WorkerState state) {
        return state.rowBatch.size() >= starRocksProperties.getMaxBatchSizeRows();
    }

    private boolean reachedTimeThreshold(WorkerState state) {
        if (state.pendingOffsets.isEmpty()) return false;
        long elapsed = System.currentTimeMillis() - state.lastFlushTime;
        return elapsed >= starRocksProperties.getMaxBatchTimeMs();
    }

    private void submitAsync(List<StarRocksRow> rows, Map<TopicPartition, OffsetAndMetadata> offsets,
                             Deque<PendingLoad> inFlightLoads) {
        List<StarRocksRow> rowsCopy = List.copyOf(rows);
        Map<TopicPartition, OffsetAndMetadata> offsetsCopy = Map.copyOf(offsets);
        String label = generateDeterministicLabel(offsetsCopy);
        CompletableFuture<StreamLoadResult> future = loadWithRetryAsync(rowsCopy, label);
        inFlightLoads.addLast(new PendingLoad(label, rowsCopy.size(), offsetsCopy, future));
        log.debug("Worker {} submitted async Stream Load. label={}, rows={}, inFlight={}", workerNumber,
                label, rowsCopy.size(), inFlightLoads.size());
    }

    private void drainCompletedLoads(Deque<PendingLoad> inFlightLoads) {
        while (hasCompletedHead(inFlightLoads)) {
            PendingLoad pending = inFlightLoads.peekFirst();
            StreamLoadResult result = awaitResult(pending);
            commitOffsets(pending);
            inFlightLoads.removeFirst();
            logCompletedLoad(pending, result, inFlightLoads.size());
        }
    }

    private boolean hasCompletedHead(Deque<PendingLoad> inFlightLoads) {
        return !inFlightLoads.isEmpty() && inFlightLoads.peekFirst().future().isDone();
    }

    private StreamLoadResult awaitResult(PendingLoad pending) {
        try {
            return pending.future().join();
        } catch (CompletionException exception) {
            log.error("Async StarRocks load failed. label={}", pending.label(), exception.getCause());
            throw exception;
        }
    }

    private void commitOffsets(PendingLoad pending) {
        consumer.commitAsync(pending.offsets(), this::handleCommitResult);
    }

    private void handleCommitResult(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        if (exception != null) {
            log.error("Worker {} Kafka async commit failed. offsets={}", workerNumber, offsets, exception);
            return;
        }
        log.debug("Worker {} Kafka offsets committed: {}", workerNumber, offsets);
    }

    private void logCompletedLoad(PendingLoad pending, StreamLoadResult result, int remainingInFlight) {
        log.info("Worker {} completed Stream Load. label={}, rows={}, status={}, remainingInFlight={}",
                workerNumber, pending.label(), pending.rowCount(), result.status(), remainingInFlight);
    }

    private CompletableFuture<StreamLoadResult> loadWithRetryAsync(List<StarRocksRow> rows, String label) {
        if (!running) return CompletableFuture.failedFuture(new IllegalStateException("Worker is stopping"));
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
        log.error("Worker {} Stream Load failed. status={}, message={}, label={}. Retrying...", workerNumber,
                result.status(), result.message(), label);
    }

    private CompletableFuture<StreamLoadResult> retryLater(List<StarRocksRow> rows, String label) {
        return delay(starRocksProperties.getRetryBackoffMs()).thenCompose(ignored -> loadWithRetryAsync(rows, label));
    }

    private CompletableFuture<Void> delay(long milliseconds) {
        return CompletableFuture.runAsync(()
                -> {}, CompletableFuture.delayedExecutor(milliseconds, TimeUnit.MILLISECONDS));
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

    private void handleWakeup(WakeupException exception) {
        if (!running) return;
        log.error("Worker {} received unexpected wakeup", workerNumber, exception);
    }

    private void closeConsumer() {
        try {
            consumer.close();
        } finally {
            log.info("StarRocks consumer worker {} stopped", workerNumber);
        }
    }

    public void stop() {
        running = false;
        consumer.wakeup();
    }
}