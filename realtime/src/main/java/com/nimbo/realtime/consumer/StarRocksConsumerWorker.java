package com.nimbo.realtime.consumer;

import com.nimbo.realtime.config.KafkaProperties;
import com.nimbo.realtime.config.StarRocksProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class StarRocksConsumerWorker implements Runnable {

    private final int workerNumber;
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final KafkaProperties kafkaProperties;
    private final StarRocksBatchProcessor batchProcessor;
    private volatile boolean running = true;

    @Override
    public void run() {
        try {
            consumer.subscribe(List.of(kafkaProperties.getTopic()));
            log.info("StarRocks consumer worker {} started", workerNumber);

            while (running) {
                processIteration();
            }
        } catch (WakeupException exception) {
            if (running) {
                log.error("Worker {} received unexpected wakeup", workerNumber, exception);
            }
        } catch (Exception exception) {
            log.error("Worker {} stopped due to unexpected error", workerNumber, exception);
        } finally {
            closeConsumer();
        }
    }

    private void processIteration() {
        batchProcessor.drainCompletedLoads(consumer);

        if (batchProcessor.isBackpressured()) {
            pauseKafka();
            consumer.poll(Duration.ofMillis(100));
            return;
        }

        resumeKafka();
        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(kafkaProperties.getPollTimeoutMs()));
        batchProcessor.process(records);
        batchProcessor.flushIfNeeded();
    }

    private void pauseKafka() {
        if (consumer.assignment().isEmpty() || batchProcessor.isKafkaPaused()) return;
        consumer.pause(consumer.assignment());
        batchProcessor.markKafkaPaused();
        log.debug("Worker {} paused Kafka", workerNumber);
    }

    private void resumeKafka() {
        if (!batchProcessor.isKafkaPaused()) return;
        consumer.resume(consumer.assignment());
        batchProcessor.markKafkaResumed();
        log.debug("Worker {} resumed Kafka", workerNumber);
    }

    private void closeConsumer() {
        try {
            batchProcessor.shutdown(consumer);
        } finally {
            consumer.close();
            log.info("StarRocks consumer worker {} stopped", workerNumber);
        }
    }

    public void stop() {
        running = false;
        consumer.wakeup();
    }
}