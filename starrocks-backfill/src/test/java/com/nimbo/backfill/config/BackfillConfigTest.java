package com.nimbo.backfill.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.nimbo.backfill.exception.BackfillException;
import com.nimbo.backfill.model.BackfillMode;

public class BackfillConfigTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-09-28T12:00:00Z"), ZoneOffset.UTC);

    @Test
    void parsesValidEnvironment() {
        BackfillConfig config = BackfillConfig.fromEnvironment(validEnvironment(), CLOCK);

        assertThat(config.mode()).isEqualTo(BackfillMode.PLAN);
        assertThat(config.database()).isEqualTo("nimbo");
        assertThat(config.table()).isEqualTo("netflow");
        assertThat(config.hdfsBasePath()).isEqualTo("hdfs://team1/netflow/parquetFiles");
    }

    @Test
    void rejectsAnIntervalCrossingUtcMidnight() {
        Map<String, String> environment = validEnvironment();
        environment.put("BACKFILL_TO", "2026-09-27T01:00:00Z");

        assertThatThrownBy(() -> BackfillConfig.fromEnvironment(environment, CLOCK))
                .isInstanceOf(BackfillException.class).hasMessageContaining("one UTC day");
    }

    @Test
    void rejectsAnIntervalThatIsStillOpen() {
        Map<String, String> environment = validEnvironment();
        environment.put("BACKFILL_FROM", "2026-09-28T11:00:00Z");
        environment.put("BACKFILL_TO", "2026-09-28T11:30:00Z");

        assertThatThrownBy(() -> BackfillConfig.fromEnvironment(environment, CLOCK))
                .isInstanceOf(BackfillException.class).hasMessageContaining("too recent");
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
                .isInstanceOf(BackfillException.class).hasMessageContaining("retention window");
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
