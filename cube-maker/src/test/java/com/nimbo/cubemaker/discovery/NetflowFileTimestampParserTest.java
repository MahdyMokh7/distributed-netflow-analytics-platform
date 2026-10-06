package com.nimbo.cubemaker.discovery;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class NetflowFileTimestampParserTest {

  @Test
  void givenValidNetflowFileNameWhenParsedThenUtcTimestampIsReturned() {
    // Given: a deterministic NetFlow Parquet filename
    String fileName = "netflow-p01-2026-10-01T08-09-10-123Z-batch.parquet";

    // When: its embedded timestamp is parsed
    var timestamp = NetflowFileTimestampParser.parse(fileName);

    // Then: the exact UTC millisecond instant is returned
    assertThat(timestamp).contains(Instant.parse("2026-10-01T08:09:10.123Z"));
  }

  @Test
  void givenBoundaryPartitionNumberWhenParsedThenTimestampIsReturned() {
    // Given: the minimum accepted two-digit partition number
    String fileName = "netflow-p00-2026-01-01T00-00-00-000Z-boundary.parquet";

    // When: the filename is parsed
    var timestamp = NetflowFileTimestampParser.parse(fileName);

    // Then: the boundary filename remains valid
    assertThat(timestamp).contains(Instant.parse("2026-01-01T00:00:00Z"));
  }

  @Test
  void givenMalformedOrImpossibleFileNamesWhenParsedThenNoTimestampIsReturned() {
    // Given: names outside the producer contract
    String malformed = "netflow-p1-2026-10-01T08-09-10-123Z-batch.parquet";
    String impossibleDate = "netflow-p01-2026-13-40T25-70-70-999Z-batch.parquet";

    // When: both invalid names are parsed
    var malformedResult = NetflowFileTimestampParser.parse(malformed);
    var impossibleResult = NetflowFileTimestampParser.parse(impossibleDate);

    // Then: neither invalid name is accepted
    assertThat(malformedResult).isEmpty();
    assertThat(impossibleResult).isEmpty();
  }
}
