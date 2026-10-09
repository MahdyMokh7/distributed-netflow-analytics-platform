package com.nimbo.cubemaker.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class SparkJobContextTest {

  private static final String JOB_DESCRIPTION = "spark.job.description";
  private static final String JOB_GROUP_ID = "spark.jobGroup.id";

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .appName("spark-job-context-test")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    spark.stop();
    SparkSession.clearActiveSession();
    SparkSession.clearDefaultSession();
  }

  @Test
  void givenSparkAction_whenExecuted_thenJobContainsBatchAwareMetadata() throws Exception {
    SparkContext sparkContext = spark.sparkContext();
    UUID batchId = UUID.fromString("10000000-0000-0000-0000-000000000001");
    CountDownLatch jobStarted = new CountDownLatch(1);
    AtomicReference<Properties> jobProperties = new AtomicReference<>();
    SparkListener listener =
        new SparkListener() {
          @Override
          public void onJobStart(SparkListenerJobStart jobStart) {
            if (jobStart.properties().getProperty(JOB_GROUP_ID) != null) {
              jobProperties.compareAndSet(null, jobStart.properties());
              jobStarted.countDown();
            }
          }
        };

    sparkContext.addSparkListener(listener);
    try {
      long count =
          SparkJobContext.execute(
              spark,
              batchId,
              "traffic-count",
              "Count traffic aggregate rows",
              () -> spark.range(2).count());

      assertThat(count).isEqualTo(2L);
      assertThat(jobStarted.await(Duration.ofSeconds(10).toMillis(), TimeUnit.MILLISECONDS))
          .isTrue();
      assertThat(jobProperties.get().getProperty(JOB_GROUP_ID))
          .isEqualTo("cube-maker:" + batchId + ":traffic-count");
      assertThat(jobProperties.get().getProperty(JOB_DESCRIPTION))
          .isEqualTo("Count traffic aggregate rows [batch=" + batchId + "]");
    } finally {
      sparkContext.removeSparkListener(listener);
    }

    assertMetadataCleared(sparkContext);
  }

  @Test
  void givenDifferentActions_whenExecuted_thenGroupIdsAreUnique() {
    SparkContext sparkContext = spark.sparkContext();
    UUID batchId = UUID.fromString("10000000-0000-0000-0000-000000000002");

    String countGroupId =
        SparkJobContext.execute(
            spark,
            batchId,
            "protocol-count",
            "Count protocol aggregate rows",
            () -> sparkContext.getLocalProperty(JOB_GROUP_ID));
    String writeGroupId =
        SparkJobContext.execute(
            spark,
            batchId,
            "protocol-write",
            "Upsert protocol aggregates into PostgreSQL",
            () -> sparkContext.getLocalProperty(JOB_GROUP_ID));

    assertThat(countGroupId).isEqualTo("cube-maker:" + batchId + ":protocol-count");
    assertThat(writeGroupId).isEqualTo("cube-maker:" + batchId + ":protocol-write");
    assertThat(countGroupId).isNotEqualTo(writeGroupId);
    assertMetadataCleared(sparkContext);
  }

  @Test
  void givenFailingAction_whenExecuted_thenMetadataIsCleared() {
    SparkContext sparkContext = spark.sparkContext();
    UUID batchId = UUID.fromString("10000000-0000-0000-0000-000000000003");

    assertThatThrownBy(
            () ->
                SparkJobContext.execute(
                    spark,
                    batchId,
                    "traffic-count",
                    "Count traffic aggregate rows",
                    (Runnable) () -> {
                      throw new IllegalStateException("expected failure");
                    }))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("expected failure");

    assertMetadataCleared(sparkContext);
  }

  private void assertMetadataCleared(SparkContext sparkContext) {
    assertThat(sparkContext.getLocalProperty(JOB_GROUP_ID)).isNull();
    assertThat(sparkContext.getLocalProperty(JOB_DESCRIPTION)).isNull();
  }
}
