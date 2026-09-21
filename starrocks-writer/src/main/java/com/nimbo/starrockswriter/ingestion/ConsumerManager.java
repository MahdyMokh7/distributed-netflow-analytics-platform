package com.nimbo.starrockswriter.ingestion;

import com.nimbo.starrockswriter.config.KafkaProperties;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class ConsumerManager {

    private final KafkaProperties kafkaProperties;
    private final ConsumerWorkerFactory workerFactory;
    private final List<ConsumerWorker> workers = new ArrayList<>();
    private ExecutorService executor;

    @PostConstruct
    public void start() {
        int consumerCount = kafkaProperties.getConsumerCount();
        executor = Executors.newFixedThreadPool(consumerCount, new CustomizableThreadFactory("starrocks-worker-"));

        for (int i = 1; i <= consumerCount; i++) {
            startWorker(i);
        }

        log.info("Started {} StarRocks Kafka consumer workers", consumerCount);
    }

    private void startWorker(int workerNumber) {
        ConsumerWorker worker = workerFactory.createWorker(workerNumber);
        workers.add(worker);
        executor.submit(worker);
        log.info("Started StarRocks worker {}", workerNumber);
    }

    @PreDestroy
    public void stopWorkers() {
        log.info("Stopping {} StarRocks Kafka consumer workers", workers.size());
        workers.forEach(ConsumerWorker::stopWorker);
        shutdownExecutor();
    }

    private void shutdownExecutor() {
        if (executor == null) return;

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
        }
    }
}
