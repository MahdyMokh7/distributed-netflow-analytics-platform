package com.nimbo.realtime.consumer;

import com.nimbo.realtime.avro.AvroDeserializer;
import com.nimbo.realtime.client.StarRocksStreamLoadClient;
import com.nimbo.realtime.config.KafkaProperties;
import com.nimbo.realtime.config.StarRocksProperties;
import com.nimbo.realtime.mapper.StarRocksRecordMapper;
import com.nimbo.realtime.validation.NetflowValidationService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
public class StarRocksConsumerManager {

    private final KafkaProperties kafkaProperties;
    private final StarRocksProperties starRocksProperties;
    private final KafkaConsumerFactory consumerFactory;
    private final AvroDeserializer avroDeserializer;
    private final NetflowValidationService validationService;
    private final StarRocksRecordMapper mapper;
    private final StarRocksStreamLoadClient streamLoadClient;
    private final List<StarRocksConsumerWorker> workers = new ArrayList<>();
    private ExecutorService executor;

    @PostConstruct
    public void start() {
        int consumerCount = kafkaProperties.getConsumerCount();
        CustomizableThreadFactory threadFactory = new CustomizableThreadFactory("starrocks-worker-");
        executor = Executors.newFixedThreadPool(consumerCount, threadFactory);

        for (int workerNumber = 1; workerNumber <= consumerCount; workerNumber++) {
            startWorker(workerNumber);
        }
        log.info("Started {} StarRocks Kafka consumer workers", consumerCount);
    }

    private void startWorker(int workerNumber) {
        KafkaConsumer<byte[], byte[]> consumer = consumerFactory.create(workerNumber, kafkaProperties);
        StarRocksBatchProcessor batchProcessor = new StarRocksBatchProcessor(workerNumber, starRocksProperties,
                avroDeserializer, validationService, mapper, streamLoadClient);
        StarRocksConsumerWorker worker = new StarRocksConsumerWorker(workerNumber, consumer, kafkaProperties, batchProcessor);

        workers.add(worker);
        executor.submit(worker);
    }

    @PreDestroy
    public void stop() {
        log.info("Stopping {} StarRocks Kafka consumer workers", workers.size());
        workers.forEach(StarRocksConsumerWorker::stop);
        shutdownExecutor();
    }

    private void shutdownExecutor() {
        if (executor == null) return;
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("Workers did not shut down gracefully in time, forcing shutdown...");
                executor.shutdownNow();
            }
        } catch (InterruptedException exception) {
            log.error("Interrupted during worker shutdown, forcing termination", exception);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
