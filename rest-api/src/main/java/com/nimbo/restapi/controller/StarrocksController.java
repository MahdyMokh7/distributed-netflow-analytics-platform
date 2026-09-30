package com.nimbo.restapi.controller;

import com.nimbo.restapi.dto.request.TimeRangeRequestDto;
import com.nimbo.restapi.dto.response.ApiResponseDto;
import com.nimbo.restapi.dto.response.PaginationMetadataDto;
import com.nimbo.restapi.dto.response.Starrocks.StarrocksBandwidthResponseDto;
import com.nimbo.restapi.dto.response.Starrocks.StarrocksConnectionResponseDto;
import com.nimbo.restapi.dto.response.Starrocks.StarrocksProtocolResponseDto;
import com.nimbo.restapi.dto.response.Starrocks.StarrocksTrafficResponseDto;
import com.nimbo.restapi.service.StarRocksQueryService;
import com.nimbo.restapi.validation.ValidIpAddress;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/StarRocks")
@RequiredArgsConstructor
@Validated
@Tag(name = "StarRocks Analytics", description = "StarRocks NetFlow queries backed by StarRocks")
public class StarrocksController {

  private static final String IP_EXAMPLE = "1.0.101.34";
  private static final String START_TIME_EXAMPLE = "2026-09-18T03:00:00Z";
  private static final String END_TIME_EXAMPLE = "2026-09-21T09:00:00Z";

  private final StarRocksQueryService queryService;

  @GetMapping("/traffic/{ip}")
  @Operation(summary = "Get traffic for an IP")
  @Parameters({
    @Parameter(
        name = "startTime",
        description = "Start of the query time range",
        required = true,
        in = ParameterIn.QUERY,
        example = START_TIME_EXAMPLE),
    @Parameter(
        name = "endTime",
        description = "End of the query time range",
        required = true,
        in = ParameterIn.QUERY,
        example = END_TIME_EXAMPLE)
  })
  public ResponseEntity<ApiResponseDto<List<StarrocksTrafficResponseDto>>> getTrafficByIp(
      @Parameter(description = "IP address", required = true, example = IP_EXAMPLE)
          @PathVariable
          @ValidIpAddress
          String ip,
      @Parameter(hidden = true) @Valid TimeRangeRequestDto timeRange) {
    List<StarrocksTrafficResponseDto> result =
        queryService.getTrafficByIp(ip, timeRange.getStartTime(), timeRange.getEndTime());

    return ResponseEntity.ok(ApiResponseDto.success(result));
  }

  @GetMapping("/connections/{ip}")
  @Operation(summary = "List connections for an IP")
  @Parameters({
    @Parameter(
        name = "startTime",
        description = "Start of the query time range",
        required = true,
        in = ParameterIn.QUERY,
        example = START_TIME_EXAMPLE),
    @Parameter(
        name = "endTime",
        description = "End of the query time range",
        required = true,
        in = ParameterIn.QUERY,
        example = END_TIME_EXAMPLE)
  })
  public ResponseEntity<ApiResponseDto<List<StarrocksConnectionResponseDto>>> getConnectionsByIp(
      @Parameter(description = "IP address", required = true, example = IP_EXAMPLE)
          @PathVariable
          @ValidIpAddress
          String ip,
      @Parameter(hidden = true) @Valid TimeRangeRequestDto timeRange,
      @Parameter(description = "Zero-based page number", example = "0")
          @RequestParam(defaultValue = "0")
          int page,
      @Parameter(description = "Number of connections per page", example = "20")
          @RequestParam(defaultValue = "20")
          int size) {
    Page<StarrocksConnectionResponseDto> result =
        queryService.getConnectionsByIp(
            ip, timeRange.getStartTime(), timeRange.getEndTime(), page, size);

    PaginationMetadataDto pagination =
        PaginationMetadataDto.of(result.getTotalElements(), result.getNumber(), result.getSize());

    return ResponseEntity.ok(ApiResponseDto.success(result.getContent(), pagination));
  }

  @GetMapping("/bandwidth/top")
  @Operation(summary = "Get IPs with highest bandwidth usage")
  @Parameters({
    @Parameter(
        name = "startTime",
        description = "Start of the query time range",
        required = true,
        in = ParameterIn.QUERY,
        example = START_TIME_EXAMPLE),
    @Parameter(
        name = "endTime",
        description = "End of the query time range",
        required = true,
        in = ParameterIn.QUERY,
        example = END_TIME_EXAMPLE)
  })
  public ResponseEntity<ApiResponseDto<List<StarrocksBandwidthResponseDto>>> getTopBandwidthIps(
      @Parameter(hidden = true) @Valid TimeRangeRequestDto timeRange,
      @Parameter(description = "Maximum number of IPs", example = "10")
          @RequestParam(defaultValue = "10")
          int limit) {
    List<StarrocksBandwidthResponseDto> result =
        queryService.getTopBandwidthIps(timeRange.getStartTime(), timeRange.getEndTime(), limit);

    return ResponseEntity.ok(ApiResponseDto.success(result));
  }

  @GetMapping("/protocols")
  @Operation(summary = "Get most commonly used protocols")
  @Parameters({
    @Parameter(
        name = "startTime",
        description = "Start of the query time range",
        required = true,
        in = ParameterIn.QUERY,
        example = START_TIME_EXAMPLE),
    @Parameter(
        name = "endTime",
        description = "End of the query time range",
        required = true,
        in = ParameterIn.QUERY,
        example = END_TIME_EXAMPLE)
  })
  public ResponseEntity<ApiResponseDto<List<StarrocksProtocolResponseDto>>> getProtocolUsage(
      @Parameter(hidden = true) @Valid TimeRangeRequestDto timeRange) {
    List<StarrocksProtocolResponseDto> result =
        queryService.getProtocolUsage(timeRange.getStartTime(), timeRange.getEndTime());

    return ResponseEntity.ok(ApiResponseDto.success(result));
  }
}
