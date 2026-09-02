package com.nimbo.realtime.streamload;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.Map;

@Component
public class StreamLoadLabelGenerator {

    public String generateLabel(int workerNumber, Map<TopicPartition, OffsetAndMetadata> offsets) {
        StringBuilder builder = new StringBuilder("nimbo-w").append(workerNumber);

        offsets.entrySet().stream()
                .sorted(Comparator.comparingInt(entry -> entry.getKey().partition()))
                .forEach(entry -> append(builder, entry));

        return builder.toString();
    }

    private void append(StringBuilder builder, Map.Entry<TopicPartition, OffsetAndMetadata> entry) {
        builder.append("-p").append(entry.getKey().partition())
                .append("-o").append(entry.getValue().offset());
    }
}
