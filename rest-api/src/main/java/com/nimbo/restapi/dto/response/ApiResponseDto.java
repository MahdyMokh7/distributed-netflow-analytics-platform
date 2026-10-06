package com.nimbo.restapi.dto.response;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ApiResponseDto<T> {

  private boolean success;

  private String message;

  private T data;

  private PaginationMetadataDto pagination;

  private Instant timestamp;

  public static <T> ApiResponseDto<T> success(T data) {
    return ApiResponseDto.<T>builder().success(true).data(data).timestamp(Instant.now()).build();
  }

  public static <T> ApiResponseDto<T> success(T data, PaginationMetadataDto pagination) {
    return ApiResponseDto.<T>builder()
        .success(true)
        .data(data)
        .pagination(pagination)
        .timestamp(Instant.now())
        .build();
  }

  public static <T> ApiResponseDto<T> success(String message) {
    return ApiResponseDto.<T>builder()
        .success(true)
        .message(message)
        .timestamp(Instant.now())
        .build();
  }

  public static <T> ApiResponseDto<T> error(String message) {
    return ApiResponseDto.<T>builder()
        .success(false)
        .message(message)
        .timestamp(Instant.now())
        .build();
  }
}
