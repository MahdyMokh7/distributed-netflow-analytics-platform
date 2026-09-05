package com.nimbo.ingester.consumer;

import com.nimbo.ingester.config.KafkaProperties;
import com.nimbo.ingester.deserializer.AvroDeserializer;
import com.nimbo.ingester.service.DataProcessingService;
import com.nimbo.ingester.service.DlqService;
import com.nimbo.ingester.service.NetflowValidationService;
import com.nimbo.metrics.BatchIngesterMetrics;
import com.nimbo.storage.config.HdfsProperties;
import com.nimbo.storage.config.ParquetProperties;
import com.nimbo.storage.service.NetFlowStorage;
import com.nimbo.storage.service.batching.BatchPolicy;
import com.nimbo.storage.service.hdfs.HdfsFileStorage;
import com.nimbo.storage.service.naming.FileNameGenerator;
import com.nimbo.storage.service.parquet.ParquetFileWriter;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import com.nimbo.storage.service.hdfs.HdfsFileSystemFactory;
import org.springframework.stereotype.Component;
import org.apache.avro.Schema;

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
    private final ParquetProperties parquetProperties;
    private final FileNameGenerator fileNameGenerator;
    private final HdfsProperties hdfsProperties;
    private final KafkaConsumerFactory kafkaConsumerFactory;
    private final HdfsFileSystemFactory hdfsFileSystemFactory;
    private final KafkaOffsetCommitter kafkaOffsetCommitter;
    private final Schema storageSchema;
    private final BatchIngesterMetrics metrics;

    private final List<NetflowConsumer> consumers = new ArrayList<>();
    private ExecutorService consumerExecutor;

    public ConsumerManager(
            KafkaProperties kafkaProperties,
            AvroDeserializer avroDeserializer,
            NetflowValidationService validationService,
            DlqService dlqService,
            BatchPolicy batchPolicy,
            ParquetProperties parquetProperties,
            FileNameGenerator fileNameGenerator,
            HdfsProperties hdfsProperties,
            KafkaConsumerFactory kafkaConsumerFactory,
            HdfsFileSystemFactory hdfsFileSystemFactory,
            KafkaOffsetCommitter kafkaOffsetCommitter,
            Schema storageSchema,
            BatchIngesterMetrics metrics) {
        this.kafkaProperties = kafkaProperties;
        this.avroDeserializer = avroDeserializer;
        this.validationService = validationService;
        this.dlqService = dlqService;
        this.batchPolicy = batchPolicy;
        this.parquetProperties = parquetProperties;
        this.fileNameGenerator = fileNameGenerator;
        this.hdfsProperties = hdfsProperties;
        this.kafkaConsumerFactory = kafkaConsumerFactory;
        this.hdfsFileSystemFactory = hdfsFileSystemFactory;
        this.kafkaOffsetCommitter = kafkaOffsetCommitter;
        this.storageSchema = storageSchema;
        this.metrics = metrics;
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

            int instanceId = i + 1;

            try {

                metrics.registerInstance(instanceId);
                FileSystem fs = hdfsFileSystemFactory.create(hdfsProperties);
                HdfsFileStorage fileStorage = new HdfsFileStorage(fs);
                ParquetFileWriter parquetWriter = new ParquetFileWriter(fs, parquetProperties);

                NetFlowStorage storage = new NetFlowStorage(
                        batchPolicy,
                        parquetWriter,
                        fileStorage,
                        fileNameGenerator,
                        hdfsProperties,
                        storageSchema,
                        metrics,
                        instanceId
                );

                DataProcessingService processingService = new DataProcessingService(storage);

                KafkaConsumer<byte[], byte[]> kafkaConsumer =
                        kafkaConsumerFactory.createConsumer(i + 1);
                metrics.bindKafkaConsumer(kafkaConsumer);

                NetflowConsumer consumer = new NetflowConsumer(
                        kafkaConsumer,
                        kafkaProperties,
                        avroDeserializer,
                        processingService,
                        validationService,
                        dlqService,
                        kafkaOffsetCommitter,
                        instanceId,
                        metrics
                );

                consumers.add(consumer);
                consumerExecutor.submit(consumer::start);
                log.info("Started consumer instance #{}", i + 1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Thread interrupted while creating consumer instance #{}", i + 1, e);
                break;

            } catch (Exception e) {
                log.error("Failed to create consumer instance #{}", i + 1, e);
            }
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