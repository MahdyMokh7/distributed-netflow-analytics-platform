package com.nimbo.ingester.consumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.stereotype.Component;

import com.nimbo.common.validation.NetflowValidationService;
import com.nimbo.ingester.config.KafkaProperties;
import com.nimbo.ingester.config.SchemaRegistryProperties;
import com.nimbo.ingester.deserializer.AvroDeserializer;
import com.nimbo.ingester.exception.SchemaRegistryException;
import com.nimbo.ingester.service.DataProcessingService;
import com.nimbo.ingester.service.DlqService;
import com.nimbo.metrics.BatchIngesterMetrics;
import com.nimbo.storage.config.HdfsProperties;
import com.nimbo.storage.config.ParquetProperties;
import com.nimbo.storage.service.NetFlowStorage;
import com.nimbo.storage.service.batching.BatchPolicy;
import com.nimbo.storage.service.hdfs.HdfsFileStorage;
import com.nimbo.storage.service.hdfs.HdfsFileSystemFactory;
import com.nimbo.storage.service.naming.FileNameGenerator;
import com.nimbo.storage.service.parquet.ParquetFileWriter;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class ConsumerManager {

    private final KafkaProperties kafkaProperties;
    private final SchemaRegistryProperties schemaRegistryProperties;
    private final NetflowValidationService validationService;
    private final DlqService dlqService;
    private final BatchPolicy batchPolicy;
    private final ParquetProperties parquetProperties;
    private final FileNameGenerator fileNameGenerator;
    private final HdfsProperties hdfsProperties;
    private final KafkaConsumerFactory kafkaConsumerFactory;
    private final HdfsFileSystemFactory hdfsFileSystemFactory;
    private final KafkaOffsetCommitter kafkaOffsetCommitter;
    private final BatchIngesterMetrics metrics;
    private final SchemaRegistryClient schemaRegistryClient;

    private final List<NetflowConsumer> consumers = new ArrayList<>();
    private ExecutorService consumerExecutor;

    public ConsumerManager(
            KafkaProperties kafkaProperties,
            SchemaRegistryProperties schemaRegistryProperties,
            NetflowValidationService validationService,
            DlqService dlqService,
            BatchPolicy batchPolicy,
            ParquetProperties parquetProperties,
            FileNameGenerator fileNameGenerator,
            HdfsProperties hdfsProperties,
            KafkaConsumerFactory kafkaConsumerFactory,
            HdfsFileSystemFactory hdfsFileSystemFactory,
            KafkaOffsetCommitter kafkaOffsetCommitter,
            BatchIngesterMetrics metrics,
            SchemaRegistryClient schemaRegistryClient) {
        this.kafkaProperties = kafkaProperties;
        this.schemaRegistryProperties = schemaRegistryProperties;
        this.validationService = validationService;
        this.dlqService = dlqService;
        this.batchPolicy = batchPolicy;
        this.parquetProperties = parquetProperties;
        this.fileNameGenerator = fileNameGenerator;
        this.hdfsProperties = hdfsProperties;
        this.kafkaConsumerFactory = kafkaConsumerFactory;
        this.hdfsFileSystemFactory = hdfsFileSystemFactory;
        this.kafkaOffsetCommitter = kafkaOffsetCommitter;
        this.metrics = metrics;
        this.schemaRegistryClient = schemaRegistryClient;
    }

    @PostConstruct
    public void start() {
        int instanceCount = kafkaProperties.getConsumerInstances();
        List<Schema> registeredSchemas = loadRegisteredSchemas();
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

                NetFlowStorage storage = new NetFlowStorage(batchPolicy, parquetWriter, fileStorage,
                        fileNameGenerator, hdfsProperties, metrics, instanceId, registeredSchemas);

                DataProcessingService processingService = new DataProcessingService(storage);

                KafkaConsumer<byte[], byte[]> kafkaConsumer = kafkaConsumerFactory.createConsumer(i + 1);
                metrics.bindKafkaConsumer(kafkaConsumer);

                AvroDeserializer avroDeserializer = createAvroDeserializer();

                NetflowConsumer consumer = new NetflowConsumer(kafkaConsumer, kafkaProperties, avroDeserializer,
                        processingService, validationService, dlqService, kafkaOffsetCommitter, instanceId, metrics);

                consumers.add(consumer);
                consumerExecutor.submit(consumer::start);
                log.info("Started consumer instance #{}", i + 1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Thread interrupted while creating consumer instance #{}", i + 1, e);
                break;

            } catch (IOException | RuntimeException e) {
                log.error("Failed to create consumer instance #{}", i + 1, e);
            }
        }
    }

    private List<Schema> loadRegisteredSchemas() {
        String subject = schemaRegistryProperties.getSubject();

        try {
            List<Integer> versions = new ArrayList<>(schemaRegistryClient.getAllVersions(subject));
            versions.sort(Comparator.naturalOrder());
            if (versions.isEmpty()) {
                throw new SchemaRegistryException(
                        "Schema Registry subject " + subject + " has no registered versions");
            }

            List<Schema> schemas = new ArrayList<>(versions.size());
            for (Integer version : versions) {
                int schemaId = schemaRegistryClient.getSchemaMetadata(subject, version).getId();
                ParsedSchema parsedSchema = schemaRegistryClient.getSchemaBySubjectAndId(subject, schemaId);
                if (!(parsedSchema instanceof AvroSchema avroSchema)) {
                    throw new SchemaRegistryException(
                            "Schema Registry subject " + subject + " version " + version + " is not Avro");
                }
                schemas.add(avroSchema.rawSchema());
            }

            log.info("Loaded {} Avro schema version(s) from Registry subject {}", schemas.size(), subject);
            return schemas;
        } catch (IOException | RestClientException exception) {
            throw new SchemaRegistryException(
                    "Failed to load schema history for Registry subject " + subject, exception);
        }
    }

    private AvroDeserializer createAvroDeserializer() {
        KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer();
        kafkaAvroDeserializer.configure(
                Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryProperties.getUrl()),
                false);
        return new AvroDeserializer(kafkaAvroDeserializer);
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
