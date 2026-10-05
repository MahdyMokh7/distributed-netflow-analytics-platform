package com.nimbo.starrockswriter.ingestion;

import com.nimbo.starrockswriter.config.KafkaProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@Order(1)
@RequiredArgsConstructor
public class ConsumerManager implements ApplicationRunner, SmartLifecycle {

    private final KafkaProperties kafkaProperties;
    private final ConsumerWorkerFactory workerFactory;
    private final List<ConsumerWorker> workers = new ArrayList<>();
    private ExecutorService executor;
    private volatile boolean running;

    @Override
    public void run(ApplicationArguments args) {
        start();
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }

        int consumerCount = kafkaProperties.getConsumerCount();
        executor = Executors.newFixedThreadPool(
                consumerCount,
                new CustomizableThreadFactory("starrocks-worker-")
        );

        for (int i = 1; i <= consumerCount; i++) {
            startWorker(i);
        }

        running = true;
        log.info("Started {} StarRocks Kafka consumer workers", consumerCount);
    }

    private void startWorker(int workerNumber) {
        ConsumerWorker worker = workerFactory.createWorker(workerNumber);
        workers.add(worker);
        executor.submit(worker);
        log.info("Started StarRocks worker {}", workerNumber);
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }

        running = false;
        log.info("Stopping {} StarRocks Kafka consumer workers", workers.size());
        workers.forEach(ConsumerWorker::stopWorker);
        shutdownExecutor();
        workers.clear();
    }

    @Override
    public void stop(Runnable callback) {
        try {
            stop();
        } finally {
            callback.run();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public boolean isAutoStartup() {
        return false;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    private void shutdownExecutor() {
        if (executor == null) {
            return;
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("Workers did not shutdown gracefully. Forcing shutdown.");
                executor.shutdownNow();
            }
        } catch (InterruptedException exception) {
            log.error("Interrupted during worker shutdown", exception);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        } finally {
            executor = null;
        }
    }
}
