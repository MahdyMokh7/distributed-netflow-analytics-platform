package com.nimbo.restapi.dto.request;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaginationRequestDto {

    @NotNull(message = "Page number is required")
    @Min(value = 0, message = "Page must be greater than or equal to 0")
    private Integer page;

    @NotNull(message = "Size is required")
    @Min(value = 1, message = "Size must be at least 1")
    @Max(value = 100, message = "Size must not exceed 100")
    private Integer size;

    @Builder.Default
    private String sortBy = "windowStart";

    @Builder.Default
    private String sortDirection = "DESC";
}