package com.nimbo.storage.service.naming;

import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

@Component
public final class FileNameGenerator {

    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss-SSS'Z'").withZone(ZoneOffset.UTC);

    public String generateFileName(int partition) {
        String timestamp = TIMESTAMP_FORMATTER.format(Instant.now());
        return String.format("netflow-p%02d-%s-%s.parquet", partition, timestamp, UUID.randomUUID());
    }

    public String generateTempFileName(String finalFileName) {
        return "." + finalFileName + ".tmp";
    }
}