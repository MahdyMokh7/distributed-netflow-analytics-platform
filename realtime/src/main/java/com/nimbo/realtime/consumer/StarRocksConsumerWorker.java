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

import java.io.IOException;
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

    private final int workerNumber;

    private final KafkaConsumer<byte[], byte[]> consumer;

    private final KafkaProperties kafkaProperties;
    private final StarRocksProperties starRocksProperties;

    private final AvroDeserializer avroDeserializer;
    private final NetflowValidationService validationService;
    private final StarRocksRecordMapper mapper;

    private final StarRocksStreamLoadClient streamLoadClient;

    private volatile boolean running = true;


    private record PendingLoad(
            String label,
            int rowCount,
            Map<TopicPartition, OffsetAndMetadata> offsets,
            CompletableFuture<StreamLoadResult> future
    ) {
    }


    @Override
    public void run() {

        List<StarRocksRow> rowBatch = new ArrayList<>();

        Map<TopicPartition, OffsetAndMetadata> pendingOffsets =
                new HashMap<>();

        Deque<PendingLoad> inFlightLoads =
                new ArrayDeque<>();

        long lastFlushTime =
                System.currentTimeMillis();

        boolean kafkaPaused = false;

        try {

            consumer.subscribe(
                    List.of(kafkaProperties.getTopic())
            );

            log.info(
                    "StarRocks consumer worker {} started",
                    workerNumber
            );


            while (running) {

                /*
                 * First check if previously submitted HTTP requests
                 * have finished.
                 */
                drainCompletedLoads(inFlightLoads);


                /*
                 * Backpressure.
                 *
                 * If StarRocks is slower than Kafka, don't keep
                 * allocating memory forever.
                 */
                if (inFlightLoads.size()
                        >= starRocksProperties.getMaxInFlightRequests()) {

                    if (!kafkaPaused && !consumer.assignment().isEmpty()) {

                        consumer.pause(
                                consumer.assignment()
                        );

                        kafkaPaused = true;

                        log.debug(
                                "Worker {} paused Kafka. inFlight={}",
                                workerNumber,
                                inFlightLoads.size()
                        );
                    }


                    /*
                     * Still call poll().
                     *
                     * This is important for consumer-group lifecycle,
                     * rebalances, etc.
                     *
                     * Because partitions are paused, we don't pull
                     * more application data.
                     */
                    consumer.poll(
                            Duration.ofMillis(100)
                    );

                    continue;
                }


                /*
                 * StarRocks has capacity again.
                 */
                if (kafkaPaused) {

                    consumer.resume(
                            consumer.assignment()
                    );

                    kafkaPaused = false;

                    log.debug(
                            "Worker {} resumed Kafka. inFlight={}",
                            workerNumber,
                            inFlightLoads.size()
                    );
                }


                ConsumerRecords<byte[], byte[]> records =
                        consumer.poll(
                                Duration.ofMillis(
                                        kafkaProperties.getPollTimeoutMs()
                                )
                        );


                for (ConsumerRecord<byte[], byte[]> kafkaRecord : records) {

                    GenericRecord avroRecord =
                            avroDeserializer.deserialize(
                                    kafkaRecord.value()
                            );

                    ValidationResult validation =
                            validationService.validate(
                                    avroRecord
                            );


                    TopicPartition topicPartition =
                            new TopicPartition(
                                    kafkaRecord.topic(),
                                    kafkaRecord.partition()
                            );

                    if (!validation.valid()) {
                        log.warn(
                                "Skipping invalid record. topic={}, partition={}, offset={}, reason={}",
                                kafkaRecord.topic(),
                                kafkaRecord.partition(),
                                kafkaRecord.offset(),
                                validation.reason()
                        );

                        pendingOffsets.put(
                                topicPartition,
                                new OffsetAndMetadata(
                                        kafkaRecord.offset() + 1
                                )
                        );

                        continue;
                    }

                    StarRocksRow row =
                            mapper.map(
                                    avroRecord,
                                    kafkaRecord.topic(),
                                    kafkaRecord.partition(),
                                    kafkaRecord.offset(),
                                    kafkaRecord.timestamp()
                            );

                    rowBatch.add(row);
                    pendingOffsets.put(
                            topicPartition,
                            new OffsetAndMetadata(
                                    kafkaRecord.offset() + 1
                            )
                    );
                }

                long now = System.currentTimeMillis();
                boolean sizeThresholdReached = rowBatch.size()
                                >= starRocksProperties.getMaxBatchSizeRows();

                boolean timeThresholdReached = !rowBatch.isEmpty()
                                && now - lastFlushTime
                                >= starRocksProperties.getMaxBatchTimeMs();

                if (sizeThresholdReached || timeThresholdReached) {
                    submitAsync(
                            rowBatch,
                            pendingOffsets,
                            inFlightLoads
                    );

                    rowBatch.clear();
                    pendingOffsets.clear();

                    lastFlushTime = System.currentTimeMillis();
                }
            }

        } catch (WakeupException e) {
            if (running) {
                log.error(
                        "Worker {} received unexpected wakeup",
                        workerNumber,
                        e
                );
            }
        } catch (Exception e) {
            log.error(
                    "Worker {} stopped due to unexpected error",
                    workerNumber,
                    e
            );

        } finally {
            try {
                consumer.close();
            } finally {
                log.info(
                        "StarRocks consumer worker {} stopped",
                        workerNumber
                );
            }
        }
    }

    private void submitAsync(
            List<StarRocksRow> rows,
            Map<TopicPartition, OffsetAndMetadata> offsets,
            Deque<PendingLoad> inFlightLoads
    ) {

        /*
         * Critical.
         *
         * rowBatch and pendingOffsets are reused after this method,
         * so the async request must own immutable copies.
         */
        List<StarRocksRow> rowsCopy =
                List.copyOf(rows);

        Map<TopicPartition, OffsetAndMetadata> offsetsCopy =
                Map.copyOf(offsets);


        String label =
                generateDeterministicLabel(offsetsCopy);


        CompletableFuture<StreamLoadResult> future =
                loadWithRetryAsync(
                        rowsCopy,
                        label
                );


        inFlightLoads.addLast(
                new PendingLoad(
                        label,
                        rowsCopy.size(),
                        offsetsCopy,
                        future
                )
        );


        log.debug(
                "Worker {} submitted async Stream Load. label={}, rows={}, inFlight={}",
                workerNumber,
                label,
                rowsCopy.size(),
                inFlightLoads.size()
        );
    }


    private void drainCompletedLoads(
            Deque<PendingLoad> inFlightLoads
    ) {

        /*
         * We intentionally process the queue in submission order.
         *
         * Example:
         *
         * batch 1 = offsets 0..19999
         * batch 2 = offsets 20000..39999
         *
         * If batch 2 finishes first, we MUST NOT commit 40000 yet.
         *
         * First batch 1 must become successful.
         */
        while (!inFlightLoads.isEmpty()) {

            PendingLoad pending =
                    inFlightLoads.peekFirst();


            if (!pending.future().isDone()) {
                return;
            }


            StreamLoadResult result;

            try {

                result =
                        pending.future().join();

            } catch (CompletionException e) {

                /*
                 * Retry future should normally only finish
                 * exceptionally during shutdown or an unrecoverable
                 * situation.
                 */
                throw e;
            }


            /*
             * HTTP/StarRocks succeeded.
             *
             * NOW Kafka offset may be committed.
             *
             * commitAsync means we don't block here either.
             */
            consumer.commitAsync(
                    pending.offsets(),
                    (offsets, exception) -> {

                        if (exception != null) {

                            log.error(
                                    "Worker {} Kafka async commit failed. offsets={}",
                                    workerNumber,
                                    offsets,
                                    exception
                            );

                        } else {

                            log.debug(
                                    "Worker {} Kafka offsets committed: {}",
                                    workerNumber,
                                    offsets
                            );
                        }
                    }
            );


            inFlightLoads.removeFirst();


            log.info(
                    "Worker {} completed Stream Load. label={}, rows={}, status={}, remainingInFlight={}",
                    workerNumber,
                    pending.label(),
                    pending.rowCount(),
                    result.status(),
                    inFlightLoads.size()
            );
        }
    }


    private CompletableFuture<StreamLoadResult> loadWithRetryAsync(
            List<StarRocksRow> rows,
            String label
    ) {

        if (!running) {

            return CompletableFuture.failedFuture(
                    new IllegalStateException(
                            "Worker is stopping"
                    )
            );
        }


        return streamLoadClient
                .loadAsync(
                        rows,
                        label
                )
                .handle(
                        (result, error) -> {

                            if (error == null) {

                                if (result.isSuccessful()
                                        || "Publish Timeout".equalsIgnoreCase(
                                        result.status()
                                )) {

                                    return CompletableFuture.completedFuture(
                                            result
                                    );
                                }


                                log.error(
                                        "Worker {} Stream Load failed. status={}, message={}, label={}. Retrying...",
                                        workerNumber,
                                        result.status(),
                                        result.message(),
                                        label
                                );

                            } else {

                                log.error(
                                        "Worker {} Stream Load HTTP failure. label={}. Retrying...",
                                        workerNumber,
                                        label,
                                        error
                                );
                            }


                            return delay(
                                    starRocksProperties.getRetryBackoffMs()
                            ).thenCompose(
                                    ignored ->
                                            loadWithRetryAsync(
                                                    rows,
                                                    label
                                            )
                            );
                        }
                )
                .thenCompose(
                        Function.identity()
                );
    }


    private CompletableFuture<Void> delay(long milliseconds) {

        return CompletableFuture.runAsync(
                () -> {
                },
                CompletableFuture.delayedExecutor(
                        milliseconds,
                        TimeUnit.MILLISECONDS
                )
        );
    }


    private String generateDeterministicLabel(
            Map<TopicPartition, OffsetAndMetadata> offsets
    ) {

        StringBuilder builder =
                new StringBuilder("nimbo-w")
                        .append(workerNumber);


        offsets.entrySet()
                .stream()
                .sorted(
                        Comparator.comparingInt(
                                entry ->
                                        entry.getKey().partition()
                        )
                )
                .forEach(
                        entry ->
                                builder
                                        .append("-p")
                                        .append(
                                                entry.getKey().partition()
                                        )
                                        .append("-o")
                                        .append(
                                                entry.getValue().offset()
                                        )
                );


        return builder.toString();
    }


    public void stop() {

        running = false;

        consumer.wakeup();
    }
}