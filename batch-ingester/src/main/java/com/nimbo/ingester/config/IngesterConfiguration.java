package com.nimbo.ingester.config;

import com.nimbo.ingester.deserializer.AvroDeserializer;
import org.apache.avro.Schema;
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
    public Schema avroSchema() {
        try (InputStream schemaStream =
                     new ClassPathResource(AVRO_SCHEMA_PATH).getInputStream()) {

            return new Schema.Parser().parse(schemaStream);

        } catch (IOException e) {
            throw new IllegalStateException(
                    "Failed to load Avro schema from: " + AVRO_SCHEMA_PATH, e
            );
        }
    }

    @Bean
    public Schema storageSchema(Schema avroSchema) {
        Schema.Field processTimeField = new Schema.Field(
                "PROCESS_TIME",
                Schema.createUnion(
                        Schema.create(Schema.Type.NULL),
                        Schema.create(Schema.Type.LONG)
                ),
                "Time when the record was ingested into HDFS (UTC epoch microseconds).",
                (Object) null
        );

        java.util.List<Schema.Field> fields = new java.util.ArrayList<>();
        for (Schema.Field field : avroSchema.getFields()) {
            Schema.Field copy = new Schema.Field(
                    field.name(),
                    field.schema(),
                    field.doc(),
                    field.defaultVal()
            );
            fields.add(copy);
        }

        fields.add(processTimeField);

        return Schema.createRecord(
                avroSchema.getName(),
                avroSchema.getDoc(),
                avroSchema.getNamespace(),
                avroSchema.isError(),
                fields
        );
    }

    @Bean
    public AvroDeserializer avroDeserializer(Schema avroSchema) {
        return new AvroDeserializer(avroSchema);
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
        );
        configs.put(
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
