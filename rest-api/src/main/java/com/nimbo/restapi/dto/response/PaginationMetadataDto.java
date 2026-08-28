package com.nimbo.restapi.dto.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

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
        return PaginationMetadataDto.builder()
                .totalElements(totalElements)
                .totalPages(totalPages)
                .page(page)
                .size(size)
                .first(page == 0)
                .last(page >= totalPages - 1)
                .numberOfElements(Math.min(size, totalElements - (long) page * size))
                .build();
    }
}