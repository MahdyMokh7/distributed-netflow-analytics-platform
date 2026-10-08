package com.nimbo.starrockswriter.ingestion;

import com.nimbo.common.validation.NetflowValidationService;
import com.nimbo.starrockswriter.config.KafkaProperties;
import com.nimbo.starrockswriter.config.SchemaRegistryProperties;
import com.nimbo.starrockswriter.config.StarRocksProperties;
import com.nimbo.starrockswriter.mapper.StarRocksRecordMapper;
import com.nimbo.starrockswriter.metrics.StarRocksWriterMetrics;
import com.nimbo.starrockswriter.processing.BatchBuffer;
import com.nimbo.starrockswriter.processing.BatchProcessor;
import com.nimbo.starrockswriter.processing.InFlightLoadManager;
import com.nimbo.starrockswriter.schema.StarRocksSchemaEvolutionManager;
import com.nimbo.starrockswriter.streamload.StarRocksStreamLoadClient;
import com.nimbo.starrockswriter.streamload.StreamLoadLabelGenerator;
import com.nimbo.starrockswriter.streamload.StreamLoadRetryHandler;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import java.util.Properties;
import lombok.RequiredArgsConstructor;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ConsumerWorkerFactory {

  private final KafkaProperties kafkaProperties;
  private final SchemaRegistryProperties schemaRegistryProperties;
  private final StarRocksProperties starRocksProperties;
  private final NetflowValidationService validationService;
  private final StarRocksRecordMapper mapper;
  private final StarRocksSchemaEvolutionManager schemaEvolutionManager;
  private final StarRocksStreamLoadClient streamLoadClient;
  private final StarRocksWriterMetrics metrics;
  private final DlqPublisher dlqPublisher;

  public ConsumerWorker createWorker(int workerNumber) {
    KafkaConsumer<byte[], GenericRecord> consumer = createKafkaConsumer();
    metrics.registerWorker(workerNumber);
    metrics.bindKafkaConsumer(workerNumber, consumer);
    BatchBuffer batchBuffer = new BatchBuffer(starRocksProperties);
    InFlightLoadManager inFlightLoadManager =
        new InFlightLoadManager(starRocksProperties, workerNumber, metrics);
    StreamLoadRetryHandler retryHandler =
        new StreamLoadRetryHandler(starRocksProperties, streamLoadClient, workerNumber, metrics);
    StreamLoadLabelGenerator labelGenerator = new StreamLoadLabelGenerator();

    BatchProcessor processor = new BatchProcessor(workerNumber, validationService, mapper, schemaEvolutionManager,
            batchBuffer, inFlightLoadManager, retryHandler, labelGenerator, metrics);

    return new ConsumerWorker(workerNumber, consumer, kafkaProperties, processor, metrics, dlqPublisher);
  }

  private KafkaConsumer<byte[], GenericRecord> createKafkaConsumer() {
    Properties props = new Properties();

    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getConsumerGroup());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, kafkaProperties.getMaxPollRecords());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaProperties.getAutoOffsetReset());
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, kafkaProperties.getClientId());
    props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, kafkaProperties.getMaxPartitionFetchBytes());

    props.put("schema.registry.url", schemaRegistryProperties.getUrl());
    props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
    props.put("http.connect.timeout.ms", schemaRegistryProperties.getConnectTimeoutMs());
    props.put("http.read.timeout.ms", schemaRegistryProperties.getRequestTimeoutMs());

    return new KafkaConsumer<>(props);
  }
}
