package com.nimbo.starrockswriter.ingestion;

import com.nimbo.common.validation.NetflowValidationService;
import com.nimbo.starrockswriter.avro.AvroDeserializer;
import com.nimbo.starrockswriter.streamload.StarRocksStreamLoadClient;
import com.nimbo.starrockswriter.config.KafkaProperties;
import com.nimbo.starrockswriter.config.StarRocksProperties;
import com.nimbo.starrockswriter.mapper.StarRocksRecordMapper;
import com.nimbo.starrockswriter.metrics.StarRocksWriterMetrics;
import com.nimbo.starrockswriter.processing.BatchBuffer;
import com.nimbo.starrockswriter.processing.BatchProcessor;
import com.nimbo.starrockswriter.processing.InFlightLoadManager;
import com.nimbo.starrockswriter.streamload.StreamLoadLabelGenerator;
import com.nimbo.starrockswriter.streamload.StreamLoadRetryHandler;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
@RequiredArgsConstructor
public class ConsumerWorkerFactory {

    private final KafkaProperties kafkaProperties;
    private final StarRocksProperties starRocksProperties;
    private final AvroDeserializer avroDeserializer;
    private final NetflowValidationService validationService;
    private final StarRocksRecordMapper mapper;
    private final StarRocksStreamLoadClient streamLoadClient;
    private final StarRocksWriterMetrics metrics;

    public ConsumerWorker createWorker(int workerNumber) {
        KafkaConsumer<byte[], byte[]> consumer = createKafkaConsumer();
        metrics.registerWorker(workerNumber);
        metrics.bindKafkaConsumer(workerNumber, consumer);
        BatchBuffer batchBuffer = new BatchBuffer(starRocksProperties);
        InFlightLoadManager inFlightLoadManager = new InFlightLoadManager(starRocksProperties, workerNumber, metrics);
        StreamLoadRetryHandler retryHandler = new StreamLoadRetryHandler(starRocksProperties, streamLoadClient, workerNumber, metrics);
        StreamLoadLabelGenerator labelGenerator = new StreamLoadLabelGenerator();

        BatchProcessor processor = new BatchProcessor(
                workerNumber, avroDeserializer, validationService, mapper,
                batchBuffer, inFlightLoadManager, retryHandler, labelGenerator, metrics
        );

        return new ConsumerWorker(workerNumber, consumer, kafkaProperties, processor, metrics);
    }

    private KafkaConsumer<byte[], byte[]> createKafkaConsumer() {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getConsumerGroup());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, kafkaProperties.getMaxPollRecords());

        return new KafkaConsumer<>(props);
    }
}
