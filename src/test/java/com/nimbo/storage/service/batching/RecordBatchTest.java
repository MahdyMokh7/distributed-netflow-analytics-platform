package com.nimbo.storage.service.batching;

import com.nimbo.ingester.model.IngestedRecord;
import com.nimbo.storage.model.TopicPartitionKey;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class RecordBatchTest {

    private GenericRecord createRecord() {
        Schema schema = new Schema.Parser().parse("""
            {
              "type": "record",
              "name": "TestRecord",
              "fields": [
                {
                  "name": "value",
                  "type": "string"
                }
              ]
            }
            """);

        GenericRecord record = new GenericData.Record(schema);
        record.put("value", "test");

        return record;
    }

    private IngestedRecord createIngestedRecord(
            String topic,
            int partition,
            long offset,
            int size
    ) {
        return new IngestedRecord(
                createRecord(),
                topic,
                partition,
                offset,
                size
        );
    }

    @Test
    void constructorShouldRejectNullRecords() {
        assertThrows(
                NullPointerException.class,
                () -> new RecordBatch(
                        null,
                        Instant.now(),
                        100
                )
        );
    }

    @Test
    void constructorShouldRejectNullCreatedAt() {
        List<IngestedRecord> records = List.of(
                createIngestedRecord("netflow", 0, 10, 100)
        );

        assertThrows(
                NullPointerException.class,
                () -> new RecordBatch(
                        records,
                        null,
                        100
                )
        );
    }

    @Test
    void constructorShouldRejectEmptyRecords() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new RecordBatch(
                        List.of(),
                        Instant.now(),
                        0
                )
        );
    }

    @Test
    void constructorShouldRejectNegativeTotalSize() {
        List<IngestedRecord> records = List.of(
                createIngestedRecord("netflow", 0, 10, 100)
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> new RecordBatch(
                        records,
                        Instant.now(),
                        -1
                )
        );
    }

    @Test
    void shouldReturnCorrectRecordsSizeAndTimestamp() {
        IngestedRecord record1 =
                createIngestedRecord("netflow", 0, 10, 100);

        IngestedRecord record2 =
                createIngestedRecord("netflow", 0, 11, 150);

        Instant createdAt = Instant.now();

        RecordBatch batch = new RecordBatch(
                List.of(record1, record2),
                createdAt,
                250
        );

        assertEquals(2, batch.size());
        assertEquals(250, batch.getTotalSizeBytes());
        assertEquals(createdAt, batch.getCreatedAt());
        assertEquals(
                List.of(record1, record2),
                batch.getRecords()
        );
    }

    @Test
    void shouldMakeRecordsListImmutable() {
        IngestedRecord record =
                createIngestedRecord("netflow", 0, 10, 100);

        RecordBatch batch = new RecordBatch(
                List.of(record),
                Instant.now(),
                100
        );

        assertThrows(
                UnsupportedOperationException.class,
                () -> batch.getRecords().clear()
        );
    }

}

