package com.nimbo.starrockswriter.ingestion;

import com.nimbo.starrockswriter.config.KafkaProperties;
import com.nimbo.starrockswriter.processing.BatchProcessor;
import com.nimbo.starrockswriter.metrics.StarRocksWriterMetrics;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.List;

@Slf4j
@RequiredArgsConstructor
public class ConsumerWorker implements Runnable {

    private final int workerNumber;
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final KafkaProperties kafkaProperties;
    private final BatchProcessor batchProcessor;
    private final StarRocksWriterMetrics metrics;
    private volatile boolean running = true;

    @Override
    public void run() {
        try {
            consumer.subscribe(List.of(kafkaProperties.getTopic()));
            metrics.workerStarted(workerNumber);
            log.info("StarRocks consumer worker {} started", workerNumber);

            while (running) {
                processIteration();
            }
        } catch (WakeupException exception) {
            if (running) {
                metrics.workerException(workerNumber, "unexpected_wakeup");
                log.error("Worker {} received unexpected wakeup", workerNumber, exception);
            }
        } catch (Exception exception) {
            metrics.workerException(workerNumber, "unexpected_exception");
            log.error("Worker {} stopped due to unexpected error", workerNumber, exception);
        } finally {
            closeConsumer();
        }
    }

    private void processIteration() {
        batchProcessor.drainCompletedLoads(consumer);

        if (batchProcessor.hasTooManyInFlightLoads()) {
            pauseKafka();
            consumerPoll(Duration.ofMillis(100), "backpressure");
            return;
        }
        if (batchProcessor.isKafkaPaused()) {
            resumeKafka();
        }

        ConsumerRecords<byte[], byte[]> records = consumerPoll(
                Duration.ofMillis(kafkaProperties.getPollTimeoutMs()), "normal");
        metrics.polled(workerNumber, records.count());
        batchProcessor.processRecords(records);
        batchProcessor.flushIfNeeded();
    }

    private ConsumerRecords<byte[], byte[]> consumerPoll(Duration timeout, String type) {
        Timer.Sample sample = metrics.startPollTimer();
        try {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(timeout);
            metrics.pollCompleted(workerNumber, type, sample);
            return records;
        } catch (RuntimeException exception) {
            metrics.pollCompleted(workerNumber, type, sample);
            throw exception;
        }
    }

    private void pauseKafka() {
        if (consumer.assignment().isEmpty() || batchProcessor.isKafkaPaused()) return;
        consumer.pause(consumer.assignment());
        batchProcessor.markKafkaPaused();
        metrics.backpressure(workerNumber, true);
        log.debug("Worker {} paused Kafka", workerNumber);
    }

    private void resumeKafka() {
        consumer.resume(consumer.assignment());
        batchProcessor.markKafkaResumed();
        metrics.backpressure(workerNumber, false);
        log.debug("Worker {} resumed Kafka", workerNumber);
    }

    private void closeConsumer() {
        try {
            batchProcessor.shutdown(consumer);
        } catch (KafkaException exception) {
            metrics.workerException(workerNumber, "shutdown_exception");
            log.error("Kafka failure during batch processor shutdown for worker {}, topic={}, consumerGroup={}",
                    workerNumber, kafkaProperties.getTopic(), kafkaProperties.getConsumerGroup(), exception);
            throw exception;
        } finally {
            consumer.close();
            metrics.unbindKafkaConsumer(workerNumber);
            metrics.workerStopped(workerNumber);
            log.info("StarRocks consumer worker {} stopped", workerNumber);
        }
    }

    public void stopWorker() {
        running = false;
        consumer.wakeup();
    }
}
