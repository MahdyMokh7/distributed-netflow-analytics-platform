package com.nimbo.ingester.config;

import com.nimbo.ingester.deserializer.AvroDeserializer;
import org.apache.avro.Schema;
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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;

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

    @Bean(destroyMethod = "close")
    public KafkaProducer<byte[], byte[]> kafkaProducer(
            KafkaProperties properties
    ) {

        Map<String, Object> configs =
                new HashMap<>();

        configs.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                properties.getBootstrapServers()
        );

        configs.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getName()
        );configs.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class.getName()
        );

        configs.put(
                ProducerConfig.ACKS_CONFIG,
                "all"
        );

        configs.put(
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                true
        );

        return new KafkaProducer<>(configs);
    }

}