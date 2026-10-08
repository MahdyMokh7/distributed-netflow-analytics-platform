package com.nimbo.storage.naming;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class FileNameGeneratorTest {

  @Test
  void givenAKafkaTimestamp_whenFileNameIsGenerated_thenDirectoryAndNameUseTheSameUtcHour() {
    // Given: a writer opening just before a UTC hour boundary
    UUID fileId = UUID.fromString("50000000-0000-0000-0000-000000000001");
    FileNameGenerator generator = new FileNameGenerator(() -> fileId);

    // When: the partitioned relative file name is generated
    long kafkaTimestamp = Instant.parse("2026-09-07T13:59:59.987Z").toEpochMilli();
    String fileName = generator.generateFileName(3, kafkaTimestamp);

    // Then: date, hour, filename timestamp, partition, and identity are consistent
    assertThat(fileName)
        .isEqualTo(
            "2026-09-07/13/netflow-p03-2026-09-07T13-59-59-987Z-50000000-0000-0000-0000-000000000001.parquet");
  }

  @Test
  void givenAPartitionedFinalName_whenTempNameIsGenerated_thenBothRemainInTheSameDirectory() {
    // Given: a committed filename inside an hourly partition
    FileNameGenerator generator = new FileNameGenerator();
    String finalName = "2026-09-07/14/netflow-p01-2026-09-07T14-00-00-000Z-file.parquet";

    // When: its hidden temporary name is derived
    String temporaryName = generator.generateTempFileName(finalName);

    // Then: only the basename changes and the hourly directory is preserved
    assertThat(temporaryName)
        .isEqualTo("2026-09-07/14/.netflow-p01-2026-09-07T14-00-00-000Z-file.parquet.tmp");
  }

  @Test
  void givenAnInvalidKafkaPartition_whenFileNameIsGenerated_thenItIsRejected() {
    // Given: a generator and a negative Kafka partition
    FileNameGenerator generator = new FileNameGenerator();

    // When: a filename is requested for the invalid partition
    Throwable failure =
        org.assertj.core.api.Assertions.catchThrowable(
            () -> generator.generateFileName(-1, 1_700_000_000_000L));

    // Then: no undiscoverable filename can be produced
    assertThat(failure)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("partition");
  }
}
