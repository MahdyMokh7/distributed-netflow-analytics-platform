package com.nimbo.restapi.controller;

import com.nimbo.restapi.dto.request.BandwidthQueryRequestDto;
import com.nimbo.restapi.dto.request.TimeRangeRequestDto;
import com.nimbo.restapi.dto.response.ApiResponseDto;
import com.nimbo.restapi.dto.response.BandwidthResponseDto;
import com.nimbo.restapi.service.BandwidthQueryService;
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
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/bandwidth")
@RequiredArgsConstructor
@Slf4j
@Tag(name = "Bandwidth", description = "IP bandwidth statistics API")
public class BandwidthController {

  private final BandwidthQueryService bandwidthService;

  @GetMapping("/top")
  @Operation(
      summary = "Get top IPs by bandwidth",
      description = "Retrieves top bandwidth consuming IPs within a time range")
  @Parameters({
      @Parameter(
        name = "startTime",
        description = "Start of the query time range",
        required = true,
        in = ParameterIn.QUERY,
        example = "2026-09-07T04:47:15.772Z"),
      @Parameter(
        name = "endTime",
        description = "End of the query time range",
        required = true,
        in = ParameterIn.QUERY,
        example = "2026-09-09T04:47:15.772Z")
  })
  public ResponseEntity<ApiResponseDto<List<BandwidthResponseDto>>> getTopBandwidthIps(
      @Parameter(description = "Limit results (default: 10, valid range: 1-100)", example = "10")
          @Valid
          Integer limit,
      @Parameter(hidden = true) @Valid TimeRangeRequestDto timeRange) {
    int effectiveLimit = limit != null && limit > 0 && limit <= 100 ? limit : 10;

    log.debug(
        "Top bandwidth query for limit: {}, time range: {} to {}",
        effectiveLimit,
        timeRange.getStartTime(),
        timeRange.getEndTime());

    BandwidthQueryRequestDto request =
        BandwidthQueryRequestDto.builder()
            .limit(effectiveLimit)
            .startTime(timeRange.getStartTime())
            .endTime(timeRange.getEndTime())
            .build();

    List<BandwidthResponseDto> result = bandwidthService.getTopBandwidthIps(request);

    return ResponseEntity.ok(ApiResponseDto.success(result));
  }

  @GetMapping("/{ip}")
  @Operation(
      summary = "Get bandwidth for specific IP",
      description = "Retrieves bandwidth statistics for a specific IP within a time range")
  @Parameters({
      @Parameter(
        name = "startTime",
        description = "Start of the query time range",
        required = true,
        in = ParameterIn.QUERY,
        example = "2026-09-07T04:47:15.772Z"),
      @Parameter(
        name = "endTime",
        description = "End of the query time range",
        required = true,
        in = ParameterIn.QUERY,
        example = "2026-09-09T04:47:15.772Z")
  })
  public ResponseEntity<ApiResponseDto<BandwidthResponseDto>> getBandwidthByIp(
      @Parameter(description = "IP address", required = true, example = "192.168.1.152")
          @PathVariable
          @ValidIpAddress
          String ip,
      @Parameter(hidden = true) @Valid TimeRangeRequestDto timeRange) {
    log.debug(
        "Bandwidth query for IP: {}, time range: {} to {}",
        ip,
        timeRange.getStartTime(),
        timeRange.getEndTime());

    BandwidthQueryRequestDto request =
        BandwidthQueryRequestDto.builder()
            .ip(ip)
            .startTime(timeRange.getStartTime())
            .endTime(timeRange.getEndTime())
            .build();

    BandwidthResponseDto result = bandwidthService.queryBandwidthByIp(ip, request);

    return ResponseEntity.ok(ApiResponseDto.success(result));
  }
}
