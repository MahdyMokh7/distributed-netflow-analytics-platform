package com.nimbo.backfill.service;

import static com.nimbo.backfill.config.BackfillConfigTest.validEnvironment;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.nimbo.backfill.config.BackfillConfig;
import com.nimbo.backfill.model.DailyBackfillPlan;

class BackfillPlannerTest {

    @Test
    void selectsOnlyHoursTouchedByTheInterval() {
        Clock clock = Clock.fixed(Instant.parse("2026-09-28T12:00:00Z"), ZoneOffset.UTC);
        BackfillConfig config = BackfillConfig.fromEnvironment(validEnvironment(), clock);

        DailyBackfillPlan plan = new BackfillPlanner().createPlans(config).get(0);

        assertThat(plan.hdfsPaths()).containsExactly("hdfs://team1/netflow/parquetFiles/2026-09-26/10/*.parquet",
                "hdfs://team1/netflow/parquetFiles/2026-09-26/11/*.parquet");
        assertThat(plan.runId()).isEqualTo("bf_20260926_101500_000_20260926_120000_000");
        assertThat(plan.stagingPartitionName()).isEqualTo("tmp_bf_20260926_101500_000_20260926_120000_000");
        assertThat(plan.fromMicros()).isEqualTo(plan.fromMillis() * 1_000L);
        assertThat(plan.toMicros()).isEqualTo(plan.toMillis() * 1_000L);
    }

    @Test
    void keepsAnExactHalfHourIntervalInsideOneHourlyPath() {
        var environment = validEnvironment();
        environment.put("BACKFILL_TO", "2026-09-26T10:45:00Z");
        Clock clock = Clock.fixed(Instant.parse("2026-09-28T12:00:00Z"), ZoneOffset.UTC);

        DailyBackfillPlan plan =
                new BackfillPlanner().createPlans(BackfillConfig.fromEnvironment(environment, clock)).get(0);

        assertThat(plan.from()).isEqualTo(Instant.parse("2026-09-26T10:15:00Z"));
        assertThat(plan.to()).isEqualTo(Instant.parse("2026-09-26T10:45:00Z"));
        assertThat(plan.hdfsPaths()).containsExactly("hdfs://team1/netflow/parquetFiles/2026-09-26/10/*.parquet");
    }

    @Test
    void splitsAMultiDayIntervalIntoDailyPlans() {
        var environment = validEnvironment();
        environment.put("BACKFILL_FROM", "2026-09-26T20:00:00Z");
        environment.put("BACKFILL_TO", "2026-09-28T02:00:00Z");
        Clock clock = Clock.fixed(Instant.parse("2026-09-28T12:00:00Z"), ZoneOffset.UTC);

        List<DailyBackfillPlan> plans =
                new BackfillPlanner().createPlans(BackfillConfig.fromEnvironment(environment, clock));

        assertThat(plans).extracting(DailyBackfillPlan::day)
                .containsExactly(LocalDate.parse("2026-09-26"), LocalDate.parse("2026-09-27"),
                        LocalDate.parse("2026-09-28"));
        assertThat(plans).extracting(DailyBackfillPlan::from)
                .containsExactly(Instant.parse("2026-09-26T20:00:00Z"), Instant.parse("2026-09-27T00:00:00Z"),
                        Instant.parse("2026-09-28T00:00:00Z"));
        assertThat(plans).extracting(DailyBackfillPlan::to)
                .containsExactly(Instant.parse("2026-09-27T00:00:00Z"), Instant.parse("2026-09-28T00:00:00Z"),
                        Instant.parse("2026-09-28T02:00:00Z"));
        assertThat(plans).extracting(plan -> plan.hdfsPaths().size()).containsExactly(4, 24, 2);
    }
}
