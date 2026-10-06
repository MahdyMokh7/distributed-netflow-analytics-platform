package com.nimbo.restapi.dto.request;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
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
  @NotBlank(message = "Sort field is required")
  private String sortBy = "windowStart";

  @Builder.Default
  @Pattern(regexp = "(?i)ASC|DESC", message = "Sort direction must be ASC or DESC")
  private String sortDirection = "DESC";
}
