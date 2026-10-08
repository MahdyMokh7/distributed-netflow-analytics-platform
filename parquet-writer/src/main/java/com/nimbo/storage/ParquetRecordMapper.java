package com.nimbo.storage;

import com.nimbo.common.netflow.NetflowMetadata;
import com.nimbo.model.IngestedRecord;
import com.nimbo.storage.exception.StorageException;
import com.nimbo.storage.schema.StorageSchemaEvolution;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

final class ParquetRecordMapper {

  private final StorageSchemaEvolution schemaEvolution;

  ParquetRecordMapper(StorageSchemaEvolution schemaEvolution) {
    this.schemaEvolution = schemaEvolution;
  }

  GenericRecord enrichWithKafkaMetadata(IngestedRecord ingestedRecord, Schema storageSchema) {
    GenericRecord enriched = new GenericData.Record(storageSchema);
    GenericRecord original = ingestedRecord.record();

    for (Schema.Field field : storageSchema.getFields()) {
      Object value = switch (field.name()) {
        case NetflowMetadata.PROCESS_TIME -> ingestedRecord.kafkaTimestamp();
        case NetflowMetadata.KAFKA_TOPIC -> ingestedRecord.topic();
        case NetflowMetadata.KAFKA_PARTITION -> ingestedRecord.partition();
        case NetflowMetadata.KAFKA_OFFSET -> ingestedRecord.offset();
        default -> schemaEvolution.valueFor(original, field);
      };
      enriched.put(field.name(), value);
    }
    return enriched;
  }

  Schema buildStorageSchema(Schema sourceSchema) {

    for (String field : NetflowMetadata.RESERVED_FIELDS) {
      if (sourceSchema.getField(field) != null) {
        throw new StorageException("Producer schema must not contain reserved field " + field);
      }
    }

    List<Schema.Field> fields = new ArrayList<>();
    sourceSchema.getFields().forEach(field -> fields.add(new Schema.Field(field, field.schema())));

    fields.add(
        storageField(
            NetflowMetadata.PROCESS_TIME,
            Schema.Type.LONG,
            NetflowMetadata.PROCESS_TIME_DOC));

    fields.add(
        storageField(
            NetflowMetadata.KAFKA_TOPIC,
            Schema.Type.STRING,
            NetflowMetadata.KAFKA_TOPIC_DOC));

    fields.add(
        storageField(
            NetflowMetadata.KAFKA_PARTITION,
            Schema.Type.INT,
            NetflowMetadata.KAFKA_PARTITION_DOC));

    fields.add(
        storageField(
            NetflowMetadata.KAFKA_OFFSET,
            Schema.Type.LONG,
            NetflowMetadata.KAFKA_OFFSET_DOC));

    return Schema.createRecord(
        sourceSchema.getName(),
        sourceSchema.getDoc(),
        sourceSchema.getNamespace(),
        sourceSchema.isError(),
        fields);
  }

  private Schema.Field storageField(String name, Schema.Type type, String doc) {
    return new Schema.Field(
        name,
        Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(type)),
        doc,
        (Object) null);
  }
}
