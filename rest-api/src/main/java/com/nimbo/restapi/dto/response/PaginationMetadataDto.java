package com.nimbo.restapi.dto.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.domain.Page;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaginationMetadataDto {

  private long totalElements;

  private int totalPages;

  private int page;

  private int size;

  private boolean first;

  private boolean last;

  private long numberOfElements;

  public static PaginationMetadataDto of(long totalElements, int page, int size) {
    int totalPages = (int) Math.ceil((double) totalElements / size);
    long remainingElements = Math.max(0, totalElements - (long) page * size);
    return PaginationMetadataDto.builder()
        .totalElements(totalElements)
        .totalPages(totalPages)
        .page(page)
        .size(size)
        .first(page == 0)
        .last(page >= totalPages - 1)
        .numberOfElements(Math.min(size, remainingElements))
        .build();
  }

  public static PaginationMetadataDto of(Page<?> page) {
    return PaginationMetadataDto.builder()
        .totalElements(page.getTotalElements())
        .totalPages(page.getTotalPages())
        .page(page.getNumber())
        .size(page.getSize())
        .first(page.isFirst())
        .last(page.isLast())
        .numberOfElements(page.getNumberOfElements())
        .build();
  }
}
