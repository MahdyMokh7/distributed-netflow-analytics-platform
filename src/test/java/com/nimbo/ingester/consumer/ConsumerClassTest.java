package com.nimbo.ingester.consumer;

import com.nimbo.ingester.model.IngestedRecord;
import com.nimbo.ingester.writer.ParquetWriterService;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.time.Duration;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ConsumerClassTest {

    @Test
    void consumerShouldReadFromKafka() throws Exception {

        ParquetWriterService writer =
                mock(ParquetWriterService.class);

        try (MockedConstruction<KafkaConsumer> mocked =
                     mockConstruction(
                             KafkaConsumer.class,
                             (mock, context) -> {

                                 when(mock.poll(any(Duration.class)))
                                         .thenThrow(
                                                 new RuntimeException("stop test")
                                         );
                             })) {

            try {
                ConsumerClass.consume(writer);
            } catch (RuntimeException e) {
                // expected - فقط برای متوقف کردن while(true)
            }

            verify(writer, never())
                    .write(any(IngestedRecord.class));
        }
    }
}
