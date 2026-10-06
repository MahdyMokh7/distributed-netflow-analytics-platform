package com.nimbo.restapi.dto.response;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;

class PaginationMetadataDtoTest {

  @Test
  void givenPartiallyFilledLastPageWhenMappedThenActualElementCountIsPreserved() {
    // Given: a final page containing two of twelve records
    PageImpl<String> page = new PageImpl<>(List.of("eleven", "twelve"), PageRequest.of(2, 5), 12);

    // When: pagination metadata is created
    PaginationMetadataDto metadata = PaginationMetadataDto.of(page);

    // Then: page boundaries and actual count match Spring Data
    assertThat(metadata.getTotalElements()).isEqualTo(12);
    assertThat(metadata.getTotalPages()).isEqualTo(3);
    assertThat(metadata.getPage()).isEqualTo(2);
    assertThat(metadata.getNumberOfElements()).isEqualTo(2);
    assertThat(metadata.isFirst()).isFalse();
    assertThat(metadata.isLast()).isTrue();
  }

  @Test
  void givenEmptyFirstPageWhenMappedThenItIsBothFirstAndLast() {
    // Given: an empty first page
    PageImpl<String> page = new PageImpl<>(List.of(), PageRequest.of(0, 20), 0);

    // When: pagination metadata is created
    PaginationMetadataDto metadata = PaginationMetadataDto.of(page);

    // Then: empty page semantics remain accurate
    assertThat(metadata.getTotalPages()).isZero();
    assertThat(metadata.getNumberOfElements()).isZero();
    assertThat(metadata.isFirst()).isTrue();
    assertThat(metadata.isLast()).isTrue();
  }
}
