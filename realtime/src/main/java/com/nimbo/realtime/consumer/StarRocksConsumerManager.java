package com.nimbo.realtime.consumer;

import com.nimbo.realtime.avro.AvroDeserializer;
import com.nimbo.realtime.config.KafkaProperties;
import com.nimbo.realtime.config.StarRocksProperties;
import com.nimbo.realtime.client.StarRocksStreamLoadClient;
import com.nimbo.realtime.mapper.StarRocksRecordMapper;
import com.nimbo.realtime.validation.NetflowValidationService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
public class StarRocksConsumerManager {

    private static final Logger log = LoggerFactory.getLogger(StarRocksConsumerManager.class);

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

        for (int i = 1; i <= consumerCount; i++) {
            KafkaConsumer<byte[], byte[]> consumer = consumerFactory.create(i);

            StarRocksConsumerWorker worker = new StarRocksConsumerWorker(
                    i, consumer, kafkaProperties, starRocksProperties,
                    avroDeserializer, validationService, mapper, streamLoadClient
            );

            workers.add(worker);
            executor.submit(worker);
        }

        log.info("Started {} StarRocks Kafka consumer workers", consumerCount);
    }

    @PreDestroy
    public void stop() {
        log.info("Stopping {} StarRocks Kafka consumer workers", workers.size());

        for (StarRocksConsumerWorker worker : workers) {
            worker.stop();
        }

        if (executor == null) {
            return;
        }

        executor.shutdown();

        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("Workers did not shut down gracefully in time, forcing shutdown...");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.error("Interrupted during worker shutdown, forcing termination", e);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}