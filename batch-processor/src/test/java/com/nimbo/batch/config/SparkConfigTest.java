package com.nimbo.batch.config;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SparkConfigTest {

    private SparkSession spark;

    @AfterEach
    void tearDown() {
        if (spark != null) {
            spark.stop();
        }
        SparkSession.clearActiveSession();
        SparkSession.clearDefaultSession();
    }

    @Test
    void shouldBuildSessionWithMasterAndAppName() {
        SparkConfig config = new SparkConfig();
        config.setMaster("local[1]");
        config.setAppName("nimbo-test-app");

        spark = config.sparkSession();

        assertThat(spark).isNotNull();
        assertThat(spark.conf().get("spark.app.name")).isEqualTo("nimbo-test-app");
        assertThat(spark.conf().get("spark.master")).isEqualTo("local[1]");
    }

    @Test
    void shouldApplyOptionalStringConfigWhenPresent() {
        SparkConfig config = new SparkConfig();
        config.setMaster("local[1]");
        config.setAppName("nimbo-test-app");
        config.setSerializer("org.apache.spark.serializer.KryoSerializer");

        spark = config.sparkSession();

        assertThat(spark.conf().get("spark.serializer"))
                .isEqualTo("org.apache.spark.serializer.KryoSerializer");
    }

    @Test
    void shouldSkipOptionalStringConfigWhenBlankOrNull() {
        SparkConfig config = new SparkConfig();
        config.setMaster("local[1]");
        config.setAppName("nimbo-test-app");
        config.setExecutorMemory("");

        spark = config.sparkSession();

        assertThat(spark.conf().getOption("spark.executor.memory").isEmpty()).isTrue();
    }

    @Test
    void shouldApplyPositiveIntConfigButSkipNonPositive() {
        SparkConfig config = new SparkConfig();
        config.setMaster("local[1]");
        config.setAppName("nimbo-test-app");
        config.setShufflePartitions(4);
        config.setExecutorCores(0); // non-positive -> should be skipped

        spark = config.sparkSession();

        assertThat(spark.conf().get("spark.sql.shuffle.partitions")).isEqualTo("4");
        assertThat(spark.conf().getOption("spark.executor.cores").isEmpty()).isTrue();
    }

    @Test
    void shouldApplyBooleanConfigWhenExplicitlySet() {
        SparkConfig config = new SparkConfig();
        config.setMaster("local[1]");
        config.setAppName("nimbo-test-app");
        config.setAdaptiveEnabled(Boolean.TRUE);

        spark = config.sparkSession();

        assertThat(spark.conf().get("spark.sql.adaptive.enabled")).isEqualTo("true");
    }
    @Test
    void shouldSkipBooleanConfigWhenNull() {
        SparkConfig config = new SparkConfig();
        config.setMaster("local[1]");
        config.setAppName("nimbo-test-app");
        config.setUiEnabled(null);

        spark = config.sparkSession();

        assertThat(spark.conf().getOption("spark.ui.enabled").isEmpty()).isTrue();
    }
}