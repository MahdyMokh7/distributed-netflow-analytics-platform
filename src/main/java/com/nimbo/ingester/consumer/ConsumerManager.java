package com.nimbo.ingester.consumer;

import com.nimbo.ingester.config.KafkaProperties;
import com.nimbo.ingester.deserializer.AvroDeserializer;
import com.nimbo.ingester.service.DataProcessingService;
import com.nimbo.ingester.service.DlqService;
import com.nimbo.ingester.service.NetflowValidationService;
import com.nimbo.storage.config.HdfsProperties;
import com.nimbo.storage.service.NetFlowStorage;
import com.nimbo.storage.service.batching.BatchPolicy;
import com.nimbo.storage.service.hdfs.FileStorage;
import com.nimbo.storage.service.naming.FileNameGenerator;
import com.nimbo.storage.service.parquet.ParquetFileWriter;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class ConsumerManager {

    private final KafkaProperties kafkaProperties;
    private final AvroDeserializer avroDeserializer;
    private final NetflowValidationService validationService;
    private final DlqService dlqService;

    private final BatchPolicy batchPolicy;
    private final ParquetFileWriter parquetFileWriter;
    private final FileStorage fileStorage;
    private final FileNameGenerator fileNameGenerator;
    private final HdfsProperties hdfsProperties;

    private final List<NetflowConsumer> consumers = new ArrayList<>();
    private ExecutorService consumerExecutor;

    public ConsumerManager(
            KafkaProperties kafkaProperties,
            AvroDeserializer avroDeserializer,
            NetflowValidationService validationService,
            DlqService dlqService,
            BatchPolicy batchPolicy,
            ParquetFileWriter parquetFileWriter,
            FileStorage fileStorage,
            FileNameGenerator fileNameGenerator,
            HdfsProperties hdfsProperties) {
        this.kafkaProperties = kafkaProperties;
        this.avroDeserializer = avroDeserializer;
        this.validationService = validationService;
        this.dlqService = dlqService;
        this.batchPolicy = batchPolicy;
        this.parquetFileWriter = parquetFileWriter;
        this.fileStorage = fileStorage;
        this.fileNameGenerator = fileNameGenerator;
        this.hdfsProperties = hdfsProperties;
    }

    @PostConstruct
    public void start() {
        int instanceCount = kafkaProperties.getConsumerInstances();
        log.info("Starting {} consumer instances for topic: {}", instanceCount, kafkaProperties.getTopic());

        consumerExecutor = Executors.newFixedThreadPool(
                instanceCount,
                r -> {
                    Thread t = new Thread(r);
                    t.setName("consumer-instance-" + t.getId());
                    t.setDaemon(true);
                    return t;
                }
        );

        for (int i = 0; i < instanceCount; i++) {
            NetFlowStorage storage = new NetFlowStorage(
                    batchPolicy,
                    parquetFileWriter,
                    fileStorage,
                    fileNameGenerator,
                    hdfsProperties
            );

            DataProcessingService processingService = new DataProcessingService(storage);

            NetflowConsumer consumer = new NetflowConsumer(
                    kafkaProperties,
                    avroDeserializer,
                    processingService,
                    validationService,
                    dlqService,
                    i + 1
            );
            consumers.add(consumer);
            consumerExecutor.submit(consumer::start);
            log.info("Started consumer instance #{}", i + 1);
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down ConsumerManager...");

        for (NetflowConsumer consumer : consumers) {
            consumer.shutdown();
        }

        if (consumerExecutor != null) {
            consumerExecutor.shutdown();
            try {
                if (!consumerExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                    consumerExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                consumerExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        log.info("ConsumerManager shutdown complete");
    }
}