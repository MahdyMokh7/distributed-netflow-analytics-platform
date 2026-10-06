package com.nimbo.backfill.service;

import com.nimbo.backfill.config.BackfillConfig;
import com.nimbo.backfill.model.DailyBackfillPlan;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public final class BackfillPlanner {

  private static final DateTimeFormatter RUN_ID_TIME =
      DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSS", Locale.ROOT).withZone(ZoneOffset.UTC);

  public List<DailyBackfillPlan> createPlans(BackfillConfig config) {
    List<DailyBackfillPlan> plans = new ArrayList<>();
    Instant from = config.from();

    while (from.isBefore(config.to())) {
      Instant nextMidnight =
          from.atZone(ZoneOffset.UTC)
              .toLocalDate()
              .plusDays(1)
              .atStartOfDay(ZoneOffset.UTC)
              .toInstant();
      Instant to = config.to().isBefore(nextMidnight) ? config.to() : nextMidnight;
      plans.add(createDailyPlan(config, from, to));
      from = to;
    }
    return plans;
  }

  private DailyBackfillPlan createDailyPlan(BackfillConfig config, Instant from, Instant to) {
    LocalDate day = from.atZone(ZoneOffset.UTC).toLocalDate();
    String runId = runId(from, to);

    return new DailyBackfillPlan(
        runId,
        config.mode(),
        day,
        from,
        to,
        "tmp_" + runId,
        hourlyPaths(config.hdfsBasePath(), from, to));
  }

  private List<String> hourlyPaths(String basePath, Instant from, Instant to) {
    List<String> paths = new ArrayList<>();
    Instant hour = from.truncatedTo(ChronoUnit.HOURS);

    while (hour.isBefore(to)) {
      int utcHour = hour.atZone(ZoneOffset.UTC).getHour();

      paths.add(
          basePath
              + "/"
              + hour.atZone(ZoneOffset.UTC).toLocalDate()
              + "/"
              + String.format(Locale.ROOT, "%02d", utcHour)
              + "/*.parquet");

      hour = hour.plus(1, ChronoUnit.HOURS);
    }
    return paths;
  }

  private String runId(Instant from, Instant to) {
    return "bf_" + RUN_ID_TIME.format(from) + "_" + RUN_ID_TIME.format(to);
  }
}
