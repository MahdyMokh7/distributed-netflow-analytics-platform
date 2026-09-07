package com.nimbo.storage.service.naming;

import org.springframework.stereotype.Component;

import java.time.Clock;
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

    private final Clock clock;
    private final Supplier<UUID> uuidSupplier;

    public FileNameGenerator() {
        this(Clock.systemUTC(), UUID::randomUUID);
    }

    FileNameGenerator(Clock clock, Supplier<UUID> uuidSupplier) {
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.uuidSupplier = Objects.requireNonNull(uuidSupplier, "uuidSupplier must not be null");
    }

    public String generateFileName(int partition) {
        if (partition < 0) {
            throw new IllegalArgumentException("partition must not be negative");
        }
        Instant openedAt = clock.instant();
        String fileName = String.format(
                "netflow-p%02d-%s-%s.parquet",
                partition,
                TIMESTAMP_FORMATTER.format(openedAt),
                uuidSupplier.get()
        );
        return String.join(
                "/",
                DATE_FORMATTER.format(openedAt),
                HOUR_FORMATTER.format(openedAt),
                fileName
        );
    }

    public String generateTempFileName(String finalFileName) {
        Objects.requireNonNull(finalFileName, "finalFileName must not be null");

        int separator = finalFileName.lastIndexOf('/');
        String parent = separator >= 0 ? finalFileName.substring(0, separator + 1) : "";
        String fileName = finalFileName.substring(separator + 1);
        return parent + "." + fileName + ".tmp";
    }
}
