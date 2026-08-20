package com.nimbo.realtime.consumer;

import com.nimbo.realtime.avro.AvroDeserializer;
import com.nimbo.realtime.config.KafkaProperties;
import com.nimbo.realtime.config.StarRocksProperties;
import com.nimbo.realtime.client.StarRocksStreamLoadClient;
import com.nimbo.realtime.mapper.StarRocksRecordMapper;
import com.nimbo.realtime.model.StarRocksRow;
import com.nimbo.realtime.model.StreamLoadResult;
import com.nimbo.realtime.validation.NetflowValidationService;
import com.nimbo.realtime.validation.ValidationResult;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class StarRocksConsumerWorker implements Runnable {

    private static final Logger log =
            LoggerFactory.getLogger(StarRocksConsumerWorker.class);

    private final int workerNumber;

    private final KafkaConsumer<byte[], byte[]> consumer;
    private final KafkaProperties kafkaProperties;
    private final StarRocksProperties starRocksProperties;

    private final AvroDeserializer avroDeserializer;
    private final NetflowValidationService validationService;
    private final StarRocksRecordMapper mapper;
    private final StarRocksStreamLoadClient streamLoadClient;

    private volatile boolean running = true;

    public StarRocksConsumerWorker(
            int workerNumber,
            KafkaConsumer<byte[], byte[]> consumer,
            KafkaProperties kafkaProperties,
            StarRocksProperties starRocksProperties,
            AvroDeserializer avroDeserializer,
            NetflowValidationService validationService,
            StarRocksRecordMapper mapper,
            StarRocksStreamLoadClient streamLoadClient
    ) {
        this.workerNumber = workerNumber;
        this.consumer = consumer;
        this.kafkaProperties = kafkaProperties;
        this.starRocksProperties = starRocksProperties;
        this.avroDeserializer = avroDeserializer;
        this.validationService = validationService;
        this.mapper = mapper;
        this.streamLoadClient = streamLoadClient;
    }

    @Override
    public void run() {

        try {

            consumer.subscribe(
                    List.of(kafkaProperties.getTopic())
            );

            log.info(
                    "StarRocks consumer worker {} started",
                    workerNumber
            );

            while (running) {

                ConsumerRecords<byte[], byte[]> records =
                        consumer.poll(
                                Duration.ofMillis(
                                        kafkaProperties.getPollTimeoutMs()
                                )
                        );

                if (records.isEmpty()) {
                    continue;
                }

                processBatch(records);
            }

        } catch (WakeupException e) {

            if (running) {
                log.error(
                        "Worker {} received unexpected wakeup",
                        workerNumber,
                        e
                );
            }

        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();

            log.info(
                    "Worker {} interrupted",
                    workerNumber
            );

        } catch (Exception e) {

            log.error(
                    "Worker {} stopped because of an unexpected error",
                    workerNumber,
                    e
            );

        } finally {

            consumer.close();

            log.info(
                    "StarRocks consumer worker {} stopped",
                    workerNumber
            );
        }
    }

    private void processBatch(
            ConsumerRecords<byte[], byte[]> records
    ) throws Exception {



        long batchStart = System.nanoTime();

        long transformStart = System.nanoTime();

        List<StarRocksRow> rows =
                new ArrayList<>(records.count());

        for (ConsumerRecord<byte[], byte[]> kafkaRecord : records) {

            GenericRecord avroRecord =
                    avroDeserializer.deserialize(
                            kafkaRecord.value()
                    );

            ValidationResult validation =
                    validationService.validate(avroRecord);

            if (!validation.valid()) {

                throw new IllegalStateException(
                        "Invalid NetFlow record. "
                                + "topic="
                                + kafkaRecord.topic()
                                + ", partition="
                                + kafkaRecord.partition()
                                + ", offset="
                                + kafkaRecord.offset()
                                + ", reason="
                                + validation.reason()
                );

            }

            StarRocksRow row =
                    mapper.map(
                            avroRecord,
                            kafkaRecord.topic(),
                            kafkaRecord.partition(),
                            kafkaRecord.offset(),
                            kafkaRecord.timestamp()
                    );

            rows.add(row);
        }
        long transformMs =
                (System.nanoTime() - transformStart)
                        / 1_000_000;

        long loadStart = System.nanoTime();

        loadWithRetry(rows);

        long loadMs =
                (System.nanoTime() - loadStart)
                        / 1_000_000;
        long commitStart = System.nanoTime();

        consumer.commitSync();

        long commitMs =
                (System.nanoTime() - commitStart)
                        / 1_000_000;


        long totalMs =
                (System.nanoTime() - batchStart)
                        / 1_000_000;

        log.info(
                "Worker {} batch timing: rows={}, transform={}ms, load={}ms, commit={}ms, total={}ms",
                workerNumber,
                rows.size(),
                transformMs,
                loadMs,
                commitMs,
                totalMs
        );

    }

    private void loadWithRetry(
            List<StarRocksRow> rows
    ) throws InterruptedException {

        while (running) {

            String label =
                    "nimbo-realtime-worker-"
                            + workerNumber
                            + "-"
                            + UUID.randomUUID();

            try {

                long start =
                        System.currentTimeMillis();

                StreamLoadResult result =
                        streamLoadClient.load(
                                rows,
                                label
                        );

                long elapsed =
                        System.currentTimeMillis() - start;

                log.info(
                        "Worker {} StarRocks timing: rows={}, "
                                + "clientTime={}ms, loadTime={}ms, "
                                + "beginTxn={}ms, read={}ms, "
                                + "write={}ms, publish={}ms",
                        workerNumber,
                        rows.size(),
                        elapsed,
                        result.loadTimeMs(),
                        result.beginTxnTimeMs(),
                        result.readDataTimeMs(),
                        result.writeDataTimeMs(),
                        result.commitAndPublishTimeMs()
                );

                if (result.isSuccessful()) {

                    log.info(
                            "Worker {} StarRocks Stream Load succeeded. "
                                    + "label={}, rows={}",
                            workerNumber,
                            label,
                            rows.size()
                    );

                    return;
                }

                if ("Publish Timeout".equalsIgnoreCase(
                        result.status()
                )) {

                    log.warn(
                            "Worker {} received Publish Timeout. label={}",
                            workerNumber,
                            label
                    );

                    return;
                }

                log.error(
                        "Worker {} StarRocks Stream Load failed. "
                                + "status={}, message={}, label={}. Retrying...",
                        workerNumber,
                        result.status(),
                        result.message(),
                        label
                );

            } catch (IOException e) {

                log.error(
                        "Worker {} StarRocks request failed. "
                                + "label={}. Retrying...",
                        workerNumber,
                        label,
                        e
                );
            }

            Thread.sleep(
                    starRocksProperties.getRetryBackoffMs()
            );
        }

        throw new InterruptedException(
                "Worker is shutting down"
        );
    }

    public void stop() {

        running = false;

        consumer.wakeup();
    }
}