package com.nimbo.realtime.ingestion;

import com.nimbo.realtime.avro.AvroDeserializer;
import com.nimbo.realtime.streamload.StarRocksStreamLoadClient;
import com.nimbo.realtime.config.KafkaProperties;
import com.nimbo.realtime.config.StarRocksProperties;
import com.nimbo.realtime.mapper.StarRocksRecordMapper;
import com.nimbo.realtime.processing.BatchBuffer;
import com.nimbo.realtime.processing.BatchProcessor;
import com.nimbo.realtime.processing.InFlightLoadManager;
import com.nimbo.realtime.streamload.StreamLoadLabelGenerator;
import com.nimbo.realtime.streamload.StreamLoadRetryHandler;
import com.nimbo.realtime.validation.NetflowValidationService;
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

    public ConsumerWorker createWorker(int workerNumber) {
        KafkaConsumer<byte[], byte[]> consumer = createKafkaConsumer();
        BatchBuffer batchBuffer = new BatchBuffer(starRocksProperties);
        InFlightLoadManager inFlightLoadManager = new InFlightLoadManager(starRocksProperties);
        StreamLoadRetryHandler retryHandler = new StreamLoadRetryHandler(starRocksProperties, streamLoadClient);
        StreamLoadLabelGenerator labelGenerator = new StreamLoadLabelGenerator();

        BatchProcessor processor = new BatchProcessor(
                workerNumber, avroDeserializer, validationService, mapper,
                batchBuffer, inFlightLoadManager, retryHandler, labelGenerator
        );

        return new ConsumerWorker(workerNumber, consumer, kafkaProperties, processor);
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
