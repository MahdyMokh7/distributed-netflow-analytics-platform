package com.nimbo.batch.writer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.sql.Statement;
import java.util.function.Supplier;

@Component
@Slf4j
public class BatchExecutor  implements Serializable {

    private static final long serialVersionUID = 1L;

    public int[] executeWithRetry(Supplier<int[]> operation, String tableName,
                                  int maxRetries, long initialDelayMs, long maxDelayMs) {
        return executeWithRetry(operation, tableName, maxRetries, initialDelayMs, maxDelayMs, () -> { });
    }

    public int[] executeWithRetry(Supplier<int[]> operation, String tableName,
                                  int maxRetries, long initialDelayMs, long maxDelayMs,
                                  Runnable retryAction) {
        int attempts = 0;
        Exception lastException = null;

        while (attempts < maxRetries) {
            try {
                int[] results = operation.get();
                verifyAllRowsAffected(results);
                return results;

            } catch (Exception e) {
                lastException = e;
                attempts++;

                if (shouldRetry(attempts, maxRetries)) {
                    retryAction.run();
                    long delay = calculateRetryDelay(attempts, initialDelayMs, maxDelayMs);
                    log.warn(
                            "Batch execution failed for table: {} (attempt {}/{}), retrying in {}ms",
                            tableName,
                            attempts,
                            maxRetries,
                            delay,
                            e
                    );
                    sleep(delay);
                }
            }
        }

        throw new RuntimeException(
                "Batch execution failed after " + maxRetries + " attempts for table: " + tableName,
                lastException
        );
    }

    private void verifyAllRowsAffected(int[] results) {
        for (int result : results) {
            if (result != 1 && result != Statement.SUCCESS_NO_INFO) {
                throw new RuntimeException("Batch write incomplete: expected success, got " + result);
            }
        }
    }

    private boolean shouldRetry(int attempts, int maxRetries) {
        return attempts < maxRetries;
    }

    private long calculateRetryDelay(int attempt, long initialDelayMs, long maxDelayMs) {
        return Math.min(
                initialDelayMs * (long) Math.pow(2.0, (double) attempt - 1),
                maxDelayMs
        );
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Retry backoff interrupted", e);
        }
    }

    public long sumResults(int[] results) {
        long total = 0;
        for (int result : results) {
            total += result == Statement.SUCCESS_NO_INFO ? 1 : result;
        }
        return total;
    }
}
