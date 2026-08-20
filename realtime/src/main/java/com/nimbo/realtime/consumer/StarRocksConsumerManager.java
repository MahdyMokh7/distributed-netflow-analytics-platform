package com.nimbo.realtime.consumer;


import com.nimbo.realtime.avro.AvroDeserializer;
import com.nimbo.realtime.config.KafkaProperties;
import com.nimbo.realtime.config.StarRocksProperties;
import com.nimbo.realtime.client.StarRocksStreamLoadClient;
import com.nimbo.realtime.mapper.StarRocksRecordMapper;
import com.nimbo.realtime.validation.NetflowValidationService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Component
public class StarRocksConsumerManager {

    private static final Logger log =
            LoggerFactory.getLogger(
                    StarRocksConsumerManager.class
            );

    private final KafkaProperties kafkaProperties;
    private final StarRocksProperties starRocksProperties;

    private final KafkaConsumerFactory consumerFactory;

    private final AvroDeserializer avroDeserializer;
    private final NetflowValidationService validationService;
    private final StarRocksRecordMapper mapper;
    private final StarRocksStreamLoadClient streamLoadClient;

    private final List<StarRocksConsumerWorker> workers =
            new ArrayList<>();

    private ExecutorService executor;

    public StarRocksConsumerManager(
            KafkaProperties kafkaProperties,
            StarRocksProperties starRocksProperties,
            KafkaConsumerFactory consumerFactory,
            AvroDeserializer avroDeserializer,
            NetflowValidationService validationService,
            StarRocksRecordMapper mapper,
            StarRocksStreamLoadClient streamLoadClient
    ) {
        this.kafkaProperties = kafkaProperties;
        this.starRocksProperties = starRocksProperties;
        this.consumerFactory = consumerFactory;
        this.avroDeserializer = avroDeserializer;
        this.validationService = validationService;
        this.mapper = mapper;
        this.streamLoadClient = streamLoadClient;
    }

    @PostConstruct
    public void start() {

        int consumerCount =
                kafkaProperties.getConsumerCount();

        executor =
                Executors.newFixedThreadPool(
                        consumerCount
                );

        for (int i = 1; i <= consumerCount; i++) {

            KafkaConsumer<byte[], byte[]> consumer =
                    consumerFactory.create(i);

            StarRocksConsumerWorker worker =
                    new StarRocksConsumerWorker(
                            i,
                            consumer,
                            kafkaProperties,
                            starRocksProperties,
                            avroDeserializer,
                            validationService,
                            mapper,
                            streamLoadClient
                    );

            workers.add(worker);

            executor.submit(worker);
        }

        log.info(
                "Started {} StarRocks Kafka consumer workers",
                consumerCount
        );
    }

    @PreDestroy
    public void stop() {

        log.info(
                "Stopping {} StarRocks Kafka consumer workers",
                workers.size()
        );

        for (StarRocksConsumerWorker worker : workers) {
            worker.stop();
        }

        if (executor == null) {
            return;
        }

        executor.shutdown();

        try {

            if (!executor.awaitTermination(
                    10,
                    TimeUnit.SECONDS
            )) {
                executor.shutdownNow();
            }

        } catch (InterruptedException e) {

            executor.shutdownNow();

            Thread.currentThread().interrupt();
        }
    }
}