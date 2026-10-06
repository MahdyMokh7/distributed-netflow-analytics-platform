package com.nimbo.backfill.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

class BackfillMetricsTest {

  @Test
  void combinesHourlyMetrics() {
    BackfillMetrics first = metrics(2, 10, 1_000L, 2_000L);
    BackfillMetrics second = metrics(3, 20, 2_001L, 3_000L);

    BackfillMetrics combined = first.combine(second);

    assertThat(combined.rowCount()).isEqualTo(5);
    assertThat(combined.inBytes()).isEqualByComparingTo("30");
    assertThat(combined.minimumTimestamp()).isEqualTo(1_000L);
    assertThat(combined.maximumTimestamp()).isEqualTo(3_000L);
  }

  @Test
  void comparesNumbersWithoutDependingOnDecimalScale() {
    BackfillMetrics first = metrics(2, 10, 1_000L, 2_000L);
    BackfillMetrics second =
        new BackfillMetrics(
            2,
            new BigDecimal("10.0"),
            new BigDecimal("20.00"),
            new BigDecimal("30.000"),
            new BigDecimal("40.0"),
            1_000L,
            2_000L);

    assertThat(first.sameDataAs(second)).isTrue();
  }

  @Test
  void rejectsNegativeRowsAndIncompleteTimestampBounds() {
    assertThatThrownBy(
            () ->
                new BackfillMetrics(
                    -1,
                    BigDecimal.ZERO,
                    BigDecimal.ZERO,
                    BigDecimal.ZERO,
                    BigDecimal.ZERO,
                    null,
                    null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("rowCount must not be negative");
    assertThatThrownBy(
            () ->
                new BackfillMetrics(
                    1,
                    BigDecimal.ZERO,
                    BigDecimal.ZERO,
                    BigDecimal.ZERO,
                    BigDecimal.ZERO,
                    1_000L,
                    null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("minimumTimestamp and maximumTimestamp must both be null or non-null");
    assertThatThrownBy(
            () ->
                new BackfillMetrics(
                    1,
                    BigDecimal.ZERO,
                    BigDecimal.ZERO,
                    BigDecimal.ZERO,
                    BigDecimal.ZERO,
                    null,
                    2_000L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("minimumTimestamp and maximumTimestamp must both be null or non-null");
  }

  @Test
  void combinesEmptyAndNonEmptyMetricsInEitherOrder() {
    BackfillMetrics populated = metrics(2, 10, 1_000L, 2_000L);

    BackfillMetrics emptyFirst = BackfillMetrics.empty().combine(populated);
    BackfillMetrics emptySecond = populated.combine(BackfillMetrics.empty());

    assertThat(emptyFirst).isEqualTo(populated);
    assertThat(emptySecond).isEqualTo(populated);
  }

  private BackfillMetrics metrics(long rows, long base, long minimum, long maximum) {
    return new BackfillMetrics(
        rows,
        BigDecimal.valueOf(base),
        BigDecimal.valueOf(base * 2),
        BigDecimal.valueOf(base * 3),
        BigDecimal.valueOf(base * 4),
        minimum,
        maximum);
  }
}
