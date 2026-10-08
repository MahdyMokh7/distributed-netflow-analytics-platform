package com.nimbo.application;

import com.nimbo.config.SchemaRegistryProperties;
import com.nimbo.ingestion.exception.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class RegisteredSchemaLoader {

  private final SchemaRegistryProperties schemaRegistryProperties;
  private final SchemaRegistryClient schemaRegistryClient;

  public RegisteredSchemaLoader(
      SchemaRegistryProperties schemaRegistryProperties,
      SchemaRegistryClient schemaRegistryClient) {
    this.schemaRegistryProperties = schemaRegistryProperties;
    this.schemaRegistryClient = schemaRegistryClient;
  }

  public List<Schema> loadRegisteredSchemas() {
    String subject = schemaRegistryProperties.getSubject();

    try {
      List<Integer> versions = new ArrayList<>(schemaRegistryClient.getAllVersions(subject));
      versions.sort(Comparator.naturalOrder());
      if (versions.isEmpty()) {
        throw new SchemaRegistryException("Schema Registry subject " + subject + " has no registered versions");
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
      throw new SchemaRegistryException("Failed to load schema history for Registry subject " + subject, exception);
    }
  }
}
