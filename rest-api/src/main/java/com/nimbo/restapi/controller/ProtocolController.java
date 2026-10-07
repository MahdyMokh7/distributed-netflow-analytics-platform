package com.nimbo.restapi.controller;

import static com.nimbo.restapi.controller.PostgresOpenApiExamples.END_TIME;
import static com.nimbo.restapi.controller.PostgresOpenApiExamples.START_TIME;

import com.nimbo.restapi.dto.request.ProtocolQueryRequestDto;
import com.nimbo.restapi.dto.request.TimeRangeRequestDto;
import com.nimbo.restapi.dto.response.ApiResponseDto;
import com.nimbo.restapi.dto.response.ProtocolResponseDto;
import com.nimbo.restapi.service.ProtocolQueryService;
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
@RequestMapping("/protocols")
@RequiredArgsConstructor
@Slf4j
@Tag(name = "Protocols", description = "Network protocol usage statistics API")
public class ProtocolController {

  private final ProtocolQueryService protocolService;

  @GetMapping
  @Operation(
      summary = "Get protocol usage statistics",
      description = "Retrieves protocol usage aggregates within a time range")
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
        example = END_TIME)
  })
  public ResponseEntity<ApiResponseDto<List<ProtocolResponseDto>>> getProtocolUsage(
      @Parameter(hidden = true) @Valid TimeRangeRequestDto timeRange) {
    log.debug(
        "Protocol query for time range: {} to {}",
        timeRange.getStartTime(),
        timeRange.getEndTime());

    ProtocolQueryRequestDto request =
        ProtocolQueryRequestDto.builder()
            .startTime(timeRange.getStartTime())
            .endTime(timeRange.getEndTime())
            .build();

    List<ProtocolResponseDto> result = protocolService.queryProtocolUsage(request);

    return ResponseEntity.ok(ApiResponseDto.success(result));
  }

  @GetMapping("/{l7Protocol}")
  @Operation(
      summary = "Get protocol usage for specific protocol",
      description = "Retrieves usage statistics for a specific protocol within a time range")
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
        example = END_TIME)
  })
  public ResponseEntity<ApiResponseDto<ProtocolResponseDto>> getProtocolUsageByProtocol(
      @Parameter(
              description = "Layer 7 protocol identifier (TCP: 6.0, UDP: 5.0)",
              required = true,
              example = "5.0")
          @PathVariable
          Double l7Protocol,
      @Parameter(hidden = true) @Valid TimeRangeRequestDto timeRange) {
    log.debug(
        "Protocol query for l7Protocol: {}, time range: {} to {}",
        l7Protocol,
        timeRange.getStartTime(),
        timeRange.getEndTime());

    ProtocolQueryRequestDto request =
        ProtocolQueryRequestDto.builder()
            .l7Protocol(l7Protocol)
            .startTime(timeRange.getStartTime())
            .endTime(timeRange.getEndTime())
            .build();

    ProtocolResponseDto result = protocolService.queryProtocolUsageByProtocol(l7Protocol, request);

    return ResponseEntity.ok(ApiResponseDto.success(result));
  }
}
