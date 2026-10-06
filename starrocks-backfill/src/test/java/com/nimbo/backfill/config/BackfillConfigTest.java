package com.nimbo.backfill.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbo.backfill.exception.BackfillException;
import com.nimbo.backfill.model.BackfillMode;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class BackfillConfigTest {

  private static final Clock CLOCK =
      Clock.fixed(Instant.parse("2026-09-28T12:00:00Z"), ZoneOffset.UTC);

  @Test
  void parsesValidEnvironment() {
    BackfillConfig config = BackfillConfig.fromEnvironment(validEnvironment(), CLOCK);

    assertThat(config.mode()).isEqualTo(BackfillMode.PLAN);
    assertThat(config.database()).isEqualTo("nimbo");
    assertThat(config.table()).isEqualTo("netflow");
    assertThat(config.hdfsBasePath()).isEqualTo("hdfs://team1/netflow/parquetFiles");
  }

  @Test
  void rejectsAnIntervalThatIsStillOpen() {
    Map<String, String> environment = validEnvironment();
    environment.put("BACKFILL_FROM", "2026-09-28T11:00:00Z");
    environment.put("BACKFILL_TO", "2026-09-28T11:30:00Z");

    assertThatThrownBy(() -> BackfillConfig.fromEnvironment(environment, CLOCK))
        .isInstanceOf(BackfillException.class)
        .hasMessageContaining("too recent");
  }

  @Test
  void acceptsAnOldEnoughIntervalBeforeTheWholeDayIsClosed() {
    Map<String, String> environment = validEnvironment();
    environment.put("BACKFILL_FROM", "2026-09-28T01:00:00Z");
    environment.put("BACKFILL_TO", "2026-09-28T02:00:00Z");

    BackfillConfig config = BackfillConfig.fromEnvironment(environment, CLOCK);

    assertThat(config.from()).isEqualTo(Instant.parse("2026-09-28T01:00:00Z"));
    assertThat(config.to()).isEqualTo(Instant.parse("2026-09-28T02:00:00Z"));
  }

  @Test
  void rejectsAnIntervalOutsideStarRocksRetention() {
    Map<String, String> environment = validEnvironment();
    environment.put("BACKFILL_FROM", "2026-09-21T10:00:00Z");
    environment.put("BACKFILL_TO", "2026-09-21T11:00:00Z");

    assertThatThrownBy(() -> BackfillConfig.fromEnvironment(environment, CLOCK))
        .isInstanceOf(BackfillException.class)
        .hasMessageContaining("retention window");
  }

  @Test
  void parsesExplicitModeAndTableOverrides() {
    Map<String, String> environment = validEnvironment();
    environment.put("BACKFILL_MODE", " execute ");
    environment.put("BACKFILL_DATABASE", "archive");
    environment.put("BACKFILL_TABLE", "netflow_history");

    BackfillConfig config = BackfillConfig.fromEnvironment(environment, CLOCK);

    assertThat(config.mode()).isEqualTo(BackfillMode.EXECUTE);
    assertThat(config.database()).isEqualTo("archive");
    assertThat(config.table()).isEqualTo("netflow_history");
  }

  @Test
  void rejectsMissingAndBlankRequiredEnvironmentValues() {
    Map<String, String> missing = validEnvironment();
    missing.remove("BACKFILL_HDFS_USERNAME");
    Map<String, String> blank = validEnvironment();
    blank.put("STARROCKS_PASSWORD", "   ");

    assertThatThrownBy(() -> BackfillConfig.fromEnvironment(missing, CLOCK))
        .isInstanceOf(BackfillException.class)
        .hasMessage("BACKFILL_HDFS_USERNAME must not be blank");
    assertThatThrownBy(() -> BackfillConfig.fromEnvironment(blank, CLOCK))
        .isInstanceOf(BackfillException.class)
        .hasMessage("STARROCKS_PASSWORD must not be blank");
  }

  @Test
  void rejectsMalformedTimestampAndMode() {
    Map<String, String> malformedTimestamp = validEnvironment();
    malformedTimestamp.put("BACKFILL_FROM", "not-an-instant");
    Map<String, String> invalidMode = validEnvironment();
    invalidMode.put("BACKFILL_MODE", "DRY_RUN");

    assertThatThrownBy(() -> BackfillConfig.fromEnvironment(malformedTimestamp, CLOCK))
        .isInstanceOf(BackfillException.class)
        .hasMessage("BACKFILL_FROM must be an ISO-8601 UTC timestamp")
        .hasCauseInstanceOf(java.time.format.DateTimeParseException.class);
    assertThatThrownBy(() -> BackfillConfig.fromEnvironment(invalidMode, CLOCK))
        .isInstanceOf(BackfillException.class)
        .hasMessage("BACKFILL_MODE must be PLAN or EXECUTE")
        .hasCauseInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsInvalidAgeAndRetentionSettings() {
    Map<String, String> negativeAge = validEnvironment();
    negativeAge.put("BACKFILL_MINIMUM_AGE_MINUTES", "-1");
    Map<String, String> nonNumericAge = validEnvironment();
    nonNumericAge.put("BACKFILL_MINIMUM_AGE_MINUTES", "soon");
    Map<String, String> zeroRetention = validEnvironment();
    zeroRetention.put("BACKFILL_RETENTION_DAYS", "0");

    assertThatThrownBy(() -> BackfillConfig.fromEnvironment(negativeAge, CLOCK))
        .isInstanceOf(BackfillException.class)
        .hasMessage("BACKFILL_MINIMUM_AGE_MINUTES must be a non-negative integer");
    assertThatThrownBy(() -> BackfillConfig.fromEnvironment(nonNumericAge, CLOCK))
        .isInstanceOf(BackfillException.class)
        .hasMessage("BACKFILL_MINIMUM_AGE_MINUTES must be a non-negative integer");
    assertThatThrownBy(() -> BackfillConfig.fromEnvironment(zeroRetention, CLOCK))
        .isInstanceOf(BackfillException.class)
        .hasMessage("BACKFILL_RETENTION_DAYS must be a positive integer");
  }

  @Test
  void rejectsUnsupportedJdbcUrlAndInvalidIntervalOrder() {
    assertThatThrownBy(
            () ->
                config(
                    "jdbc:postgresql://database/nimbo",
                    BackfillMode.PLAN,
                    Instant.parse("2026-09-26T10:00:00Z"),
                    Instant.parse("2026-09-26T11:00:00Z")))
        .isInstanceOf(BackfillException.class)
        .hasMessage("STARROCKS_JDBC_URL must start with jdbc:mysql://");
    assertThatThrownBy(
            () ->
                config(
                    "jdbc:mysql://database/nimbo",
                    BackfillMode.PLAN,
                    Instant.parse("2026-09-26T11:00:00Z"),
                    Instant.parse("2026-09-26T10:00:00Z")))
        .isInstanceOf(BackfillException.class)
        .hasMessage("BACKFILL_FROM must be before BACKFILL_TO");
  }

  @Test
  void rejectsSubMillisecondPrecision() {
    assertThatThrownBy(
            () ->
                config(
                    "jdbc:mysql://database/nimbo",
                    BackfillMode.PLAN,
                    Instant.parse("2026-09-26T10:00:00.000000001Z"),
                    Instant.parse("2026-09-26T11:00:00Z")))
        .isInstanceOf(BackfillException.class)
        .hasMessage("Backfill timestamps must have millisecond precision");
  }

  @Test
  void rejectsNullModeAndIntervalBoundsClearly() {
    Instant from = Instant.parse("2026-09-26T10:00:00Z");
    Instant to = Instant.parse("2026-09-26T11:00:00Z");

    assertThatThrownBy(() -> config("jdbc:mysql://database/nimbo", null, from, to))
        .isInstanceOf(BackfillException.class)
        .hasMessage("BACKFILL_MODE must not be null");
    assertThatThrownBy(() -> config("jdbc:mysql://database/nimbo", BackfillMode.PLAN, null, to))
        .isInstanceOf(BackfillException.class)
        .hasMessage("BACKFILL_FROM must not be null");
    assertThatThrownBy(() -> config("jdbc:mysql://database/nimbo", BackfillMode.PLAN, from, null))
        .isInstanceOf(BackfillException.class)
        .hasMessage("BACKFILL_TO must not be null");
  }

  private BackfillConfig config(String jdbcUrl, BackfillMode mode, Instant from, Instant to) {
    return new BackfillConfig(
        jdbcUrl,
        "backfill",
        "secret",
        "nimbo",
        "netflow",
        "hdfs://team1/netflow/parquetFiles",
        "team1",
        mode,
        from,
        to);
  }

  public static Map<String, String> validEnvironment() {
    Map<String, String> environment = new HashMap<>();
    environment.put("STARROCKS_JDBC_URL", "jdbc:mysql://starrocks-fe:9030/nimbo");
    environment.put("STARROCKS_USERNAME", "backfill");
    environment.put("STARROCKS_PASSWORD", "secret");
    environment.put("BACKFILL_MODE", "PLAN");
    environment.put("BACKFILL_FROM", "2026-09-26T10:15:00Z");
    environment.put("BACKFILL_TO", "2026-09-26T12:00:00Z");
    environment.put("BACKFILL_HDFS_BASE_PATH", "hdfs://team1/netflow/parquetFiles");
    environment.put("BACKFILL_HDFS_USERNAME", "team1");
    return environment;
  }
}
