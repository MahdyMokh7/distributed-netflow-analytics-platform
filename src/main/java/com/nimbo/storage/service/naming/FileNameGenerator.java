package com.nimbo.storage.service.naming;

import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

@Component
public final class FileNameGenerator {

    public String generateFileName(int partition) {
        String timestamp = DateTimeFormatter.ISO_INSTANT
                .format(Instant.now())
                .replace(":", "-");

        return String.format("netflow-p%d-%s-%s.parquet", partition, timestamp, UUID.randomUUID());
    }

    public String generateTempFileName(String finalFileName) {
        return "." + finalFileName + ".tmp";
    }
}