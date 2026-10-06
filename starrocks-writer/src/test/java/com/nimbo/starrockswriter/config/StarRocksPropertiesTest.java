package com.nimbo.starrockswriter.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class StarRocksPropertiesTest {

  @Test
  void shouldStoreStarRocksConnectionProperties() {
    StarRocksProperties properties = new StarRocksProperties();

    properties.setStreamLoadBackends(List.of("backend-1", "backend-2"));
    properties.setDatabase("netflow_db");
    properties.setTable("flows");
    properties.setUsername("user");
    properties.setPassword("password");

    assertThat(properties.getStreamLoadBackends()).containsExactly("backend-1", "backend-2");
    assertThat(properties.getDatabase()).isEqualTo("netflow_db");
    assertThat(properties.getTable()).isEqualTo("flows");
    assertThat(properties.getUsername()).isEqualTo("user");
    assertThat(properties.getPassword()).isEqualTo("password");
  }

  @Test
  void shouldStoreRetryConfiguration() {
    StarRocksProperties properties = new StarRocksProperties();

    properties.setRetryBackoffMs(500);

    assertThat(properties.getRetryBackoffMs()).isEqualTo(500);
  }

  @Test
  void shouldStoreMergeCommitConfiguration() {
    StarRocksProperties properties = new StarRocksProperties();

    properties.setMergeCommitEnabled(true);
    properties.setMergeCommitAsync(true);
    properties.setMergeCommitIntervalMs(1000);
    properties.setMergeCommitParallel(4);

    assertThat(properties.isMergeCommitEnabled()).isTrue();
    assertThat(properties.isMergeCommitAsync()).isTrue();
    assertThat(properties.getMergeCommitIntervalMs()).isEqualTo(1000);
    assertThat(properties.getMergeCommitParallel()).isEqualTo(4);
  }

  @Test
  void shouldStoreBatchConfiguration() {
    StarRocksProperties properties = new StarRocksProperties();

    properties.setMaxBatchSizeRows(1000);
    properties.setMaxBatchTimeMs(5000);
    properties.setMaxInFlightRequests(10);

    assertThat(properties.getMaxBatchSizeRows()).isEqualTo(1000);
    assertThat(properties.getMaxBatchTimeMs()).isEqualTo(5000);
    assertThat(properties.getMaxInFlightRequests()).isEqualTo(10);
  }
}
