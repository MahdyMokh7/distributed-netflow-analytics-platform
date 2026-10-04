package com.nimbo.backfill.config;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.Map;

import com.nimbo.backfill.exception.BackfillException;
import com.nimbo.backfill.model.BackfillMode;

public record BackfillConfig(String jdbcUrl, String username, String password, String database, String table,
        String hdfsBasePath, String hdfsUsername, BackfillMode mode, Instant from, Instant to) {

    public BackfillConfig {
        requireText(jdbcUrl, "STARROCKS_JDBC_URL");
        requireText(username, "STARROCKS_USERNAME");
        requireText(password, "STARROCKS_PASSWORD");
        requireText(database, "BACKFILL_DATABASE");
        requireText(table, "BACKFILL_TABLE");
        requireText(hdfsBasePath, "BACKFILL_HDFS_BASE_PATH");
        requireText(hdfsUsername, "BACKFILL_HDFS_USERNAME");
        if (!jdbcUrl.startsWith("jdbc:mysql://")) {
            throw new BackfillException("STARROCKS_JDBC_URL must start with jdbc:mysql://");
        }
        if (!from.isBefore(to)) {
            throw new BackfillException("BACKFILL_FROM must be before BACKFILL_TO");
        }
        if (!Instant.ofEpochMilli(from.toEpochMilli()).equals(from)
                || !Instant.ofEpochMilli(to.toEpochMilli()).equals(to)) {
            throw new BackfillException("Backfill timestamps must have millisecond precision");
        }
    }

    public static BackfillConfig fromEnvironment(Map<String, String> environment, Clock clock) {

        Instant from = parseInstant(required(environment, "BACKFILL_FROM"), "BACKFILL_FROM");
        Instant to = parseInstant(required(environment, "BACKFILL_TO"), "BACKFILL_TO");

        int minimumAgeMinutes = parseNonNegativeInt(environment.getOrDefault("BACKFILL_MINIMUM_AGE_MINUTES", "60"),
                "BACKFILL_MINIMUM_AGE_MINUTES");
        int retentionDays = parsePositiveInt(environment.getOrDefault("BACKFILL_RETENTION_DAYS", "7"),
                "BACKFILL_RETENTION_DAYS");

        if (to.isAfter(clock.instant().minus(Duration.ofMinutes(minimumAgeMinutes)))) {
            throw new BackfillException("BACKFILL_TO is too recent; the interval must be closed for at least "
                    + minimumAgeMinutes + " minutes");
        }
        LocalDate earliestRetainedDay =
                clock.instant().atZone(ZoneOffset.UTC).toLocalDate().minusDays(retentionDays - 1L);
        if (from.atZone(ZoneOffset.UTC).toLocalDate().isBefore(earliestRetainedDay)) {
            throw new BackfillException("BACKFILL_FROM is outside the " + retentionDays
                    + "-day StarRocks retention window; earliest retained UTC day is " + earliestRetainedDay);
        }

        return new BackfillConfig(required(environment, "STARROCKS_JDBC_URL"),
                required(environment, "STARROCKS_USERNAME"),
                required(environment, "STARROCKS_PASSWORD"),
                environment.getOrDefault("BACKFILL_DATABASE", "nimbo"),
                environment.getOrDefault("BACKFILL_TABLE", "netflow"),
                required(environment, "BACKFILL_HDFS_BASE_PATH"),
                required(environment, "BACKFILL_HDFS_USERNAME"),
                parseMode(environment.getOrDefault("BACKFILL_MODE", "PLAN")), from, to);
    }

    private static String required(Map<String, String> environment, String name) {
        String value = environment.get(name);
        requireText(value, name);
        return value.trim();
    }

    private static Instant parseInstant(String value, String name) {
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException exception) {
            throw new BackfillException(name + " must be an ISO-8601 UTC timestamp", exception);
        }
    }

    private static BackfillMode parseMode(String value) {
        try {
            return BackfillMode.valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException exception) {
            throw new BackfillException("BACKFILL_MODE must be PLAN or EXECUTE", exception);
        }
    }

    private static int parseNonNegativeInt(String value, String name) {
        try {
            int parsed = Integer.parseInt(value);
            if (parsed < 0) {
                throw new NumberFormatException("negative value");
            }
            return parsed;
        } catch (NumberFormatException exception) {
            throw new BackfillException(name + " must be a non-negative integer", exception);
        }
    }

    private static int parsePositiveInt(String value, String name) {
        int parsed = parseNonNegativeInt(value, name);
        if (parsed == 0) {
            throw new BackfillException(name + " must be a positive integer");
        }
        return parsed;
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new BackfillException(name + " must not be blank");
        }
    }
}
