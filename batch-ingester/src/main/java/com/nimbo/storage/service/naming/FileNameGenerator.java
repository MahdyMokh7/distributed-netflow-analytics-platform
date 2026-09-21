package com.nimbo.storage.service.naming;

import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;

@Component
public final class FileNameGenerator {

    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH-mm-ss-SSS'Z'").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter HOUR_FORMATTER =
            DateTimeFormatter.ofPattern("HH").withZone(ZoneOffset.UTC);

    private final Supplier<UUID> uuidSupplier;

    public FileNameGenerator() {
        this(UUID::randomUUID);
    }

    FileNameGenerator(Supplier<UUID> uuidSupplier) {
        this.uuidSupplier = Objects.requireNonNull(uuidSupplier, "uuidSupplier must not be null");
    }

    public String generateFileName(int partition, long kafkaTimestamp) {

        if (partition < 0) {
            throw new IllegalArgumentException("partition must not be negative");
        }

        Instant eventTime = Instant.ofEpochMilli(kafkaTimestamp);
        String fileName = String.format(
                "netflow-p%02d-%s-%s.parquet",
                partition,
                TIMESTAMP_FORMATTER.format(eventTime),
                uuidSupplier.get()
        );

        return String.join("/", DATE_FORMATTER.format(eventTime), HOUR_FORMATTER.format(eventTime), fileName);
    }

    public String generateTempFileName(String finalFileName) {
        Objects.requireNonNull(finalFileName, "finalFileName must not be null");

        int separator = finalFileName.lastIndexOf('/');
        String parent = separator >= 0 ? finalFileName.substring(0, separator + 1) : "";
        String fileName = finalFileName.substring(separator + 1);
        return parent + "." + fileName + ".tmp";
    }
}
