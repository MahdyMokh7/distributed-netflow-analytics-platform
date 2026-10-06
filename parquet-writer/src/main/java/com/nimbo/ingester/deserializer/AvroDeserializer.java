package com.nimbo.ingester.deserializer;

import com.nimbo.ingester.exception.DeserializationException;
import com.nimbo.ingester.exception.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.io.Closeable;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Objects;
import javax.net.ssl.SSLException;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.SerializationException;

public class AvroDeserializer implements Closeable {

  private final KafkaAvroDeserializer kafkaAvroDeserializer;

  public AvroDeserializer(KafkaAvroDeserializer kafkaAvroDeserializer) {
    this.kafkaAvroDeserializer = Objects.requireNonNull(kafkaAvroDeserializer);
  }

  public GenericRecord deserialize(String topic, byte[] data) {
    if (data == null) {
      throw new DeserializationException("Avro payload must not be null");
    }

    try {
      return requireGenericRecord(kafkaAvroDeserializer.deserialize(topic, data));
    } catch (AuthenticationException | AuthorizationException | RetriableException exception) {
      throw new SchemaRegistryException("Schema Registry is unavailable", exception);
    } catch (SerializationException exception) {
      throw classifySerializationFailure(exception);
    }
  }

  private GenericRecord requireGenericRecord(Object decoded) {
    if (decoded instanceof GenericRecord genericRecord) {
      return genericRecord;
    }
    throw new DeserializationException("Decoded Avro value is not a record");
  }

  private RuntimeException classifySerializationFailure(SerializationException exception) {
    if (isRegistryFailure(exception)) {
      return new SchemaRegistryException("Schema Registry lookup failed", exception);
    }
    return new DeserializationException("Failed to deserialize Avro record", exception);
  }

  private boolean isRegistryFailure(Throwable exception) {
    Throwable current = exception.getCause();
    while (current != null) {
      if ((current instanceof RestClientException restClientException
              && isRetryableRegistryStatus(restClientException.getStatus()))
          || current instanceof UnknownHostException
          || current instanceof SocketException
          || current instanceof SSLException) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  private boolean isRetryableRegistryStatus(int status) {
    return status == 401 || status == 403 || status == 429 || status >= 500;
  }

  @Override
  public void close() {
    kafkaAvroDeserializer.close();
  }
}
