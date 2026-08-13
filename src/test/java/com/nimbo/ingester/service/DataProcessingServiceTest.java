package com.nimbo.ingester.service;

import com.nimbo.ingester.model.IngestedRecord;
import com.nimbo.storage.model.TopicPartitionKey;
import com.nimbo.storage.service.NetFlowStorage;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("DataProcessingService Tests")
class DataProcessingServiceTest {

    private static final String SCHEMA_JSON = """
            {
                "type": "record",
                "name": "NetFlowV2Record",
                "namespace": "org.nimbo.netflow.avro",
                "fields": [
                    {"name": "IPV4_SRC_ADDR", "type": "string"}
                ]
            }
            """;

    @Mock
    private NetFlowStorage netFlowStorage;

    private DataProcessingService service;

    @BeforeEach
    void setUp() {
        service = new DataProcessingService(netFlowStorage);
    }

    @Test
    @DisplayName("Should process record successfully when storage accepts it")
    void shouldProcessRecordSuccessfully() {
        IngestedRecord record = createTestRecord();
        when(netFlowStorage.accept(any(IngestedRecord.class)))
                .thenReturn(Optional.empty());

        service.processRecord(record);

        ArgumentCaptor<IngestedRecord> captor = ArgumentCaptor.forClass(IngestedRecord.class);
        verify(netFlowStorage).accept(captor.capture());
        assertThat(captor.getValue()).isSameAs(record);
    }

    @Test
    @DisplayName("Should handle storage returning offsets")
    void shouldHandleStorageReturningOffsets() {
        IngestedRecord record = createTestRecord();
        when(netFlowStorage.accept(any(IngestedRecord.class)))
                .thenReturn(Optional.of(Map.of()));

        service.processRecord(record);

        verify(netFlowStorage).accept(any(IngestedRecord.class));
    }

    @Test
    @DisplayName("Should propagate exception when storage fails")
    void shouldPropagateExceptionWhenStorageFails() {
        IngestedRecord record = createTestRecord();
        when(netFlowStorage.accept(any(IngestedRecord.class)))
                .thenThrow(new RuntimeException("Storage failed"));

        assertThatThrownBy(() -> service.processRecord(record))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Storage failed");
    }

    @Test
    @DisplayName("Should call storage with correct record")
    void shouldCallStorageWithCorrectRecord() {
        IngestedRecord record = createTestRecord();
        when(netFlowStorage.accept(any(IngestedRecord.class)))
                .thenReturn(Optional.empty());

        service.processRecord(record);

        verify(netFlowStorage).accept(record);
    }

    @Test
    @DisplayName("Should flush storage and return offsets")
    void shouldFlushStorageAndReturnOffsets() {
        Map<TopicPartitionKey, Long> expectedOffsets = Map.of(
                new TopicPartitionKey("test-topic", 0), 43L
        );
        when(netFlowStorage.flush()).thenReturn(expectedOffsets);

        Map<TopicPartitionKey, Long> result = service.flush();

        assertThat(result).isEqualTo(expectedOffsets);
        verify(netFlowStorage).flush();
    }

    @Test
    @DisplayName("Should propagate exception when flush fails")
    void shouldPropagateExceptionWhenFlushFails() {
        when(netFlowStorage.flush())
                .thenThrow(new RuntimeException("Flush failed"));

        assertThatThrownBy(() -> service.flush())
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Flush failed");

        verify(netFlowStorage).flush();
    }

    @Test
    @DisplayName("Should return empty map when storage flush returns empty")
    void shouldReturnEmptyMapWhenStorageFlushReturnsEmpty() {
        when(netFlowStorage.flush()).thenReturn(Map.of());

        Map<TopicPartitionKey, Long> result = service.flush();

        assertThat(result).isEmpty();
        verify(netFlowStorage).flush();
    }

    private IngestedRecord createTestRecord() {
        Schema schema = new Schema.Parser().parse(SCHEMA_JSON);
        GenericRecord record = new GenericData.Record(schema);
        record.put("IPV4_SRC_ADDR", "192.168.1.1");
        return new IngestedRecord(record, "test-topic", 0, 42L, 1024);
    }
}