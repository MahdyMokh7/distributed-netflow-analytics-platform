package com.nimbo.restapi.controller;

import static com.nimbo.restapi.controller.PostgresOpenApiExamples.END_TIME;
import static com.nimbo.restapi.controller.PostgresOpenApiExamples.START_TIME;

import com.nimbo.restapi.dto.request.PaginationRequestDto;
import com.nimbo.restapi.dto.request.TimeRangeRequestDto;
import com.nimbo.restapi.dto.request.TrafficQueryRequestDto;
import com.nimbo.restapi.dto.response.ApiResponseDto;
import com.nimbo.restapi.dto.response.PaginationMetadataDto;
import com.nimbo.restapi.dto.response.TrafficResponseDto;
import com.nimbo.restapi.service.TrafficQueryService;
import com.nimbo.restapi.validation.ValidIpAddress;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/traffic")
@RequiredArgsConstructor
@Slf4j
@Tag(name = "Traffic", description = "Network traffic statistics API")
public class TrafficController {

  private final TrafficQueryService trafficService;

  @GetMapping("/{srcIp}")
  @Operation(
      summary = "Get traffic for specific source IP",
      description = "Retrieves traffic aggregates for a given source IP within a time range")
  @Parameters({
      @Parameter(
        name = "startTime",
        description = "Start of the query time range",
        required = true,
        in = ParameterIn.QUERY,
        example = START_TIME),
      @Parameter(
        name = "endTime",
        description = "End of the query time range",
        required = true,
        in = ParameterIn.QUERY,
        example = END_TIME),
      @Parameter(
        name = "page",
        description = "Zero-based page number",
        required = true,
        in = ParameterIn.QUERY,
        example = "0"),
      @Parameter(
        name = "size",
        description = "Page size (1-100)",
        required = true,
        in = ParameterIn.QUERY,
        example = "10"),
      @Parameter(
        name = "sortBy",
        description = "Response field used for sorting",
        in = ParameterIn.QUERY,
        example = "inBytes"),
      @Parameter(
        name = "sortDirection",
        description = "Sort direction (ASC or DESC)",
        in = ParameterIn.QUERY,
        example = "DESC")
  })
  public ResponseEntity<ApiResponseDto<List<TrafficResponseDto>>> getTrafficByIp(
      @Parameter(description = "Source IP address", required = true, example = "223.191.114.202")
          @PathVariable
          @ValidIpAddress
          String srcIp,
      @Parameter(hidden = true) @Valid TimeRangeRequestDto timeRange,
      @Parameter(hidden = true) @Valid PaginationRequestDto pagination) {

    log.debug(
        "Traffic query for IP: {}, time range: {} to {}, page: {}, size: {}",
        srcIp,
        timeRange.getStartTime(),
        timeRange.getEndTime(),
        pagination.getPage(),
        pagination.getSize());

    TrafficQueryRequestDto request =
        TrafficQueryRequestDto.builder()
            .srcIp(srcIp)
            .startTime(timeRange.getStartTime())
            .endTime(timeRange.getEndTime())
            .pagination(pagination)
            .build();

    Page<TrafficResponseDto> result = trafficService.queryTrafficBySrcIp(srcIp, request);

    return ResponseEntity.ok(
        ApiResponseDto.success(result.getContent(), PaginationMetadataDto.of(result)));
  }

  @GetMapping
  @Operation(
      summary = "List traffic with filters",
      description = "Retrieves traffic aggregates with optional filters and pagination")
  @Parameters({
      @Parameter(
        name = "startTime",
        description = "Start of the query time range",
        required = true,
        in = ParameterIn.QUERY,
        example = START_TIME),
      @Parameter(
        name = "endTime",
        description = "End of the query time range",
        required = true,
        in = ParameterIn.QUERY,
        example = END_TIME),
      @Parameter(
        name = "page",
        description = "Zero-based page number",
        required = true,
        in = ParameterIn.QUERY,
        example = "0"),
      @Parameter(
        name = "size",
        description = "Page size (1-100)",
        required = true,
        in = ParameterIn.QUERY,
        example = "10"),
      @Parameter(
        name = "sortBy",
        description = "Response field used for sorting",
        in = ParameterIn.QUERY,
        example = "inBytes"),
      @Parameter(
        name = "sortDirection",
        description = "Sort direction (ASC or DESC)",
        in = ParameterIn.QUERY,
        example = "DESC")
  })
  public ResponseEntity<ApiResponseDto<List<TrafficResponseDto>>> queryTraffic(
      @Parameter(description = "Optional source IP filter", example = "223.191.114.202")
          @ValidIpAddress
          String srcIp,
      @Parameter(hidden = true) @Valid TimeRangeRequestDto timeRange,
      @Parameter(hidden = true) @Valid PaginationRequestDto pagination) {
    log.debug(
        "Traffic query with filters - srcIp: {}, page: {}, size: {}",
        srcIp,
        pagination.getPage(),
        pagination.getSize());

    TrafficQueryRequestDto request =
        TrafficQueryRequestDto.builder()
            .srcIp(srcIp)
            .startTime(timeRange.getStartTime())
            .endTime(timeRange.getEndTime())
            .pagination(pagination)
            .build();

    Page<TrafficResponseDto> result = trafficService.queryTraffic(request);

    return ResponseEntity.ok(
        ApiResponseDto.success(result.getContent(), PaginationMetadataDto.of(result)));
  }
}
