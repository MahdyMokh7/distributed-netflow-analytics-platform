package com.nimbo.starrockswriter.ingestion;

import com.nimbo.starrockswriter.config.KafkaProperties;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.apache.kafka.common.errors.RecordDeserializationException.DeserializationExceptionOrigin.VALUE;
import static org.assertj.core.api.Assertions.assertThat;

class DlqPublisherTest {

    @Test
    void shouldPreserveRawBytesAndAddSourceMetadata() {
        KafkaProperties properties = new KafkaProperties();
        properties.setDlqTopic("netflow-dlq");
        DlqPublisher publisher = new DlqPublisher(null, properties);

        byte[] key = {1, 2};
        byte[] value = {0, 0, 0, 0, 3, 10, 20};
        RecordHeaders originalHeaders = new RecordHeaders();
        originalHeaders.add("trace-id", "abc".getBytes(StandardCharsets.UTF_8));

        RecordDeserializationException exception = new RecordDeserializationException(
                VALUE,
                new TopicPartition("multi-version-netflow", 2),
                42L,
                123456789L,
                TimestampType.CREATE_TIME,
                ByteBuffer.wrap(key),
                ByteBuffer.wrap(value),
                originalHeaders,
                "Error deserializing VALUE",
                new SerializationException("Unknown magic byte")
        );

        ProducerRecord<byte[], byte[]> record = publisher.createDlqRecord(exception);

        assertThat(record.topic()).isEqualTo("netflow-dlq");
        assertThat(record.timestamp()).isEqualTo(123456789L);
        assertThat(record.key()).containsExactly(key);
        assertThat(record.value()).containsExactly(value);
        assertThat(header(record, "trace-id")).isEqualTo("abc");
        assertThat(header(record, "original-topic")).isEqualTo("multi-version-netflow");
        assertThat(header(record, "original-partition")).isEqualTo("2");
        assertThat(header(record, "original-offset")).isEqualTo("42");
        assertThat(header(record, "error-type")).isEqualTo("DESERIALIZATION_ERROR");
        assertThat(header(record, "deserialization-origin")).isEqualTo("VALUE");
    }

    private String header(ProducerRecord<byte[], byte[]> record, String name) {
        Header header = record.headers().lastHeader(name);
        assertThat(header).isNotNull();
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
