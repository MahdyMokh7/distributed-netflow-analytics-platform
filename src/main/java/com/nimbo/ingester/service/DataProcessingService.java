package com.nimbo.ingester.service;

import com.nimbo.ingester.model.IngestedRecord;
import com.nimbo.storage.service.NetFlowStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class DataProcessingService {

    private static final Logger log = LoggerFactory.getLogger(DataProcessingService.class);

    private final NetFlowStorage netFlowStorage;

    public DataProcessingService(NetFlowStorage netFlowStorage) {
        this.netFlowStorage = netFlowStorage;
    }

    public void processRecord(IngestedRecord record) {
        try {
            netFlowStorage.accept(record).ifPresent(offsets -> {
                log.debug("Batch flushed to HDFS successfully");
            });
        } catch (Exception e) {
            log.error("Failed to process record: topic={}, partition={}, offset={}",
                    record.topic(), record.partition(), record.offset(), e);
            throw e;
        }
    }
}