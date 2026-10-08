package com.nimbo.application;

import com.nimbo.common.validation.NetflowValidationService;
import com.nimbo.config.HdfsProperties;
import com.nimbo.config.KafkaProperties;
import com.nimbo.config.ParquetProperties;
import com.nimbo.config.SchemaRegistryProperties;
import com.nimbo.ingestion.AvroDeserializer;
import com.nimbo.ingestion.DataProcessingService;
import com.nimbo.ingestion.DlqService;
import com.nimbo.ingestion.KafkaConsumerFactory;
import com.nimbo.ingestion.KafkaNetFlowConsumer;
import com.nimbo.ingestion.KafkaOffsetCommitter;
import com.nimbo.ingestion.NetflowRecordProcessor;
import com.nimbo.metrics.ParquetWriterMetrics;
import com.nimbo.storage.HourWindowPolicy;
import com.nimbo.storage.NetFlowParquetWriter;
import com.nimbo.storage.batching.BatchPolicy;
import com.nimbo.storage.hdfs.HdfsFileStorage;
import com.nimbo.storage.hdfs.HdfsFileSystemFactory;
import com.nimbo.storage.naming.FileNameGenerator;
import com.nimbo.storage.parquet.ParquetFileWriter;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.stereotype.Component;

@Component
public class ConsumerInstanceFactory {

  private final KafkaProperties kafkaProperties;
  private final SchemaRegistryProperties schemaRegistryProperties;
  private final NetflowValidationService validationService;
  private final DlqService dlqService;
  private final BatchPolicy batchPolicy;
  private final HourWindowPolicy hourWindowPolicy;
  private final ParquetProperties parquetProperties;
  private final FileNameGenerator fileNameGenerator;
  private final HdfsProperties hdfsProperties;
  private final KafkaConsumerFactory kafkaConsumerFactory;
  private final HdfsFileSystemFactory hdfsFileSystemFactory;
  private final KafkaOffsetCommitter kafkaOffsetCommitter;
  private final ParquetWriterMetrics metrics;

  public ConsumerInstanceFactory(
      KafkaProperties kafkaProperties,
      SchemaRegistryProperties schemaRegistryProperties,
      NetflowValidationService validationService,
      DlqService dlqService,
      BatchPolicy batchPolicy,
      HourWindowPolicy hourWindowPolicy,
      ParquetProperties parquetProperties,
      FileNameGenerator fileNameGenerator,
      HdfsProperties hdfsProperties,
      KafkaConsumerFactory kafkaConsumerFactory,
      HdfsFileSystemFactory hdfsFileSystemFactory,
      KafkaOffsetCommitter kafkaOffsetCommitter,
      ParquetWriterMetrics metrics) {
    this.kafkaProperties = kafkaProperties;
    this.schemaRegistryProperties = schemaRegistryProperties;
    this.validationService = validationService;
    this.dlqService = dlqService;
    this.batchPolicy = batchPolicy;
    this.hourWindowPolicy = hourWindowPolicy;
    this.parquetProperties = parquetProperties;
    this.fileNameGenerator = fileNameGenerator;
    this.hdfsProperties = hdfsProperties;
    this.kafkaConsumerFactory = kafkaConsumerFactory;
    this.hdfsFileSystemFactory = hdfsFileSystemFactory;
    this.kafkaOffsetCommitter = kafkaOffsetCommitter;
    this.metrics = metrics;
  }

  public KafkaNetFlowConsumer create(int instanceId, List<Schema> registeredSchemas)
      throws IOException, InterruptedException {

    metrics.registerInstance(instanceId);
    FileSystem fs = hdfsFileSystemFactory.create(hdfsProperties);
    HdfsFileStorage fileStorage = new HdfsFileStorage(fs);
    ParquetFileWriter parquetWriter = new ParquetFileWriter(fs, parquetProperties);

    NetFlowParquetWriter netFlowParquetWriter = new NetFlowParquetWriter(batchPolicy, parquetWriter, fileStorage,
        fileNameGenerator, hdfsProperties, hourWindowPolicy, metrics, instanceId, registeredSchemas);

    DataProcessingService processingService = new DataProcessingService(netFlowParquetWriter);

    KafkaConsumer<byte[], byte[]> kafkaConsumer = kafkaConsumerFactory.createConsumer(instanceId);
    metrics.bindKafkaConsumer(kafkaConsumer);

    AvroDeserializer avroDeserializer = createAvroDeserializer();
    NetflowRecordProcessor recordProcessor = new NetflowRecordProcessor(avroDeserializer, processingService,
            validationService, dlqService, instanceId, metrics);

    return new KafkaNetFlowConsumer(kafkaConsumer, kafkaProperties, avroDeserializer, processingService,
            recordProcessor, kafkaOffsetCommitter, instanceId, metrics);
  }

  private AvroDeserializer createAvroDeserializer() {
    KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer();
    kafkaAvroDeserializer.configure(
        Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryProperties.getUrl()), false);
    return new AvroDeserializer(kafkaAvroDeserializer);
  }
}
