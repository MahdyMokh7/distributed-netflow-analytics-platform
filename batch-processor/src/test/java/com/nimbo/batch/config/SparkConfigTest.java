package com.nimbo.batch.config;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class SparkConfigTest {

    @TempDir
    Path eventLogDirectory;

    @Test
    void givenProductionTuning_whenSparkStarts_thenResourceLimitsAndSqlSettingsAreApplied() {
        // Given: a local Spark configuration with production resource limits
        SparkConfig config = new SparkConfig();
        config.setMaster("local[1]");
        config.setAppName("spark-timezone-test");
        config.setSessionTimeZone("UTC");
        config.setExecutorMemory("2g");
        config.setExecutorCores(2);
        config.setNumExecutors(2);
        config.setMaxCores(4);
        config.setFilesMaxPartitionBytes("64m");
        config.setEventLogEnabled(true);
        config.setEventLogDirectory(eventLogDirectory.toUri().toString());
        config.setUiEnabled(false);

        // When: the production Spark configuration creates a session
        SparkSession spark = config.sparkSession();

        // Then: Spark receives the resource limits and SQL settings
        try {
            assertThat(spark.conf().get("spark.sql.session.timeZone")).isEqualTo("UTC");
            assertThat(spark.sessionState().conf().filesMaxPartitionBytes()).isEqualTo(64L * 1024 * 1024);
            assertThat(spark.sparkContext().getConf().get("spark.executor.memory")).isEqualTo("2g");
            assertThat(spark.sparkContext().getConf().getInt("spark.executor.cores", 0)).isEqualTo(2);
            assertThat(spark.sparkContext().getConf().getInt("spark.executor.instances", 0)).isEqualTo(2);
            assertThat(spark.sparkContext().getConf().getInt("spark.cores.max", 0)).isEqualTo(4);
            assertThat(spark.sparkContext().getConf().getBoolean("spark.eventLog.enabled", false)).isTrue();
            assertThat(spark.sparkContext().getConf().get("spark.eventLog.dir"))
                    .isEqualTo(eventLogDirectory.toUri().toString());
        } finally {
            spark.stop();
            SparkSession.clearActiveSession();
            SparkSession.clearDefaultSession();
        }
    }
}
