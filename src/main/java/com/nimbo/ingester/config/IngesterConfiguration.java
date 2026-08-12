package com.nimbo.ingester.config;

import com.nimbo.ingester.deserializer.AvroDeserializer;
import com.nimbo.storage.service.NetFlowStorage;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

@Configuration
@EnableConfigurationProperties(KafkaProperties.class)
public class IngesterConfiguration {

    private static final String AVRO_SCHEMA_PATH = "avro/NetFlowV2Record.avsc";

    @Bean
    public Schema avroSchema() throws IOException {
        try (InputStream schemaStream = new ClassPathResource(AVRO_SCHEMA_PATH).getInputStream()) {
            if (schemaStream == null) {
                throw new IllegalStateException("Avro schema not found at: " + AVRO_SCHEMA_PATH);
            }
            return new Schema.Parser().parse(schemaStream);
        }
    }

    @Bean
    public AvroDeserializer avroDeserializer(Schema schema) {
        return new AvroDeserializer(schema);
    }

    @Bean
    public KafkaConsumer<byte[], byte[]> kafkaConsumer(KafkaProperties properties) {
        Map<String, Object> configs = buildConsumerConfigs(properties);
        return new KafkaConsumer<>(configs);
    }

    private Map<String, Object> buildConsumerConfigs(KafkaProperties properties) {
        Map<String, Object> configs = new HashMap<>();

        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getConsumerGroup());
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, properties.isEnableAutoCommit());
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configs.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, properties.getMaxPollRecords());
        configs.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, properties.getSessionTimeoutMs());
        configs.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, properties.getHeartbeatIntervalMs());
        configs.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, properties.getMaxPollIntervalMs());

        if (properties.getAdditionalConfigs() != null) {
            configs.putAll(properties.getAdditionalConfigs());
        }

        return configs;
    }
}