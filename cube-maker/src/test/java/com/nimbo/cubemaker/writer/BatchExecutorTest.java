package com.nimbo.cubemaker.writer;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class BatchExecutorTest {

  private final BatchExecutor executor = new BatchExecutor();

  @Test
  void givenSuccessfulBatchWithoutPerRowCounts_whenSummed_thenEveryEntryCountsAsOneRow() {
    int[] results = {Statement.SUCCESS_NO_INFO, Statement.SUCCESS_NO_INFO};

    int[] executed = executor.executeWithRetry(() -> results, "traffic_aggregates", 1, 0, 0);

    assertThat(executor.sumResults(executed)).isEqualTo(2L);
  }

  @Test
  void givenATransientFailure_whenExecuted_thenItRetriesAndReturnsTheCompleteBatchResult() {
    // Given: an operation that fails once before returning two successful updates
    AtomicInteger attempts = new AtomicInteger();
    AtomicInteger retries = new AtomicInteger();

    // When: the operation is executed with a bounded retry policy
    int[] results =
        executor.executeWithRetry(
            () -> {
              if (attempts.incrementAndGet() == 1) {
                throw new IllegalStateException("temporary database outage");
              }
              return new int[] {1, 1};
            },
            "traffic_aggregates",
            2,
            0,
            0,
            retries::incrementAndGet);

    // Then: the second attempt succeeds and every row is confirmed
    assertThat(attempts.get()).isEqualTo(2);
    assertThat(retries.get()).isEqualTo(1);
    assertThat(results).containsExactly(1, 1);
  }

  @Test
  void givenAPartialBatchResult_whenExecuted_thenItFailsAfterTheConfiguredAttempts() {
    // Given: an operation whose database result leaves one row unconfirmed
    AtomicInteger attempts = new AtomicInteger();

    // When: the incomplete result is executed with two allowed attempts
    Throwable failure =
        org.assertj.core.api.Assertions.catchThrowable(
            () ->
                executor.executeWithRetry(
                    () -> {
                      attempts.incrementAndGet();
                      return new int[] {1, 0};
                    },
                    "connection_aggregates",
                    2,
                    0,
                    0));

    // Then: the partial write is rejected rather than counted as successful
    assertThat(failure)
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("after 2 attempts");
    assertThat(attempts.get()).isEqualTo(2);
  }

  @Test
  void givenSuccessfulBatchCounts_whenSummed_thenTheWrittenRowCountIsAccurate() {
    // Given: deterministic update counts from three JDBC batches
    int[] results = {2, 3, 5};

    // When: the writer total is calculated
    long total = executor.sumResults(results);

    // Then: every batch count contributes to the accumulated total
    assertThat(total).isEqualTo(10L);
  }
}
