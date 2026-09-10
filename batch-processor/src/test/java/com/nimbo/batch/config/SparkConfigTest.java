package com.nimbo.batch.config;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SparkConfigTest {

    @Test
    void givenConfiguredUtcTimezone_whenSparkStarts_thenSqlTimestampsUseUtc() {
        // Given: a local Spark configuration with the production timezone
        SparkConfig config = new SparkConfig();
        config.setMaster("local[1]");
        config.setAppName("spark-timezone-test");
        config.setSessionTimeZone("UTC");
        config.setUiEnabled(false);

        // When: the production Spark configuration creates a session
        SparkSession spark = config.sparkSession();

        // Then: Spark SQL interprets and renders timestamps in UTC
        try {
            assertThat(spark.conf().get("spark.sql.session.timeZone")).isEqualTo("UTC");
        } finally {
            spark.stop();
            SparkSession.clearActiveSession();
            SparkSession.clearDefaultSession();
        }
    }
}
