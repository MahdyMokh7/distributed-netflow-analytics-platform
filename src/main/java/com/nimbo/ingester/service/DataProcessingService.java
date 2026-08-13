package com.nimbo.ingester.service;

import com.nimbo.ingester.model.IngestedRecord;
import com.nimbo.storage.model.TopicPartitionKey;
import com.nimbo.storage.service.NetFlowStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;

@Service
public class DataProcessingService {

    private static final Logger log = LoggerFactory.getLogger(DataProcessingService.class);

    private final NetFlowStorage netFlowStorage;

    public DataProcessingService(NetFlowStorage netFlowStorage) {
        this.netFlowStorage = netFlowStorage;
    }

    public Optional<Map<TopicPartitionKey, Long>> processRecord(IngestedRecord record) {
        try {
            return netFlowStorage.accept(record);
        } catch (Exception e) {
            log.error("Failed to process record: topic={}, partition={}, offset={}",
                    record.topic(), record.partition(), record.offset(), e);
            throw e;
        }
    }

    // FLUSH - For graceful shutdown and rebalance
    public Map<TopicPartitionKey, Long> flush() {
        try {
            return netFlowStorage.flush();
        } catch (Exception e) {
            log.error("Failed to flush storage", e);
            throw e;
        }
    }
}