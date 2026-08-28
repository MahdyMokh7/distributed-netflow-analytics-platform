package com.nimbo.restapi.controller.v1;

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
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/traffic")
@RequiredArgsConstructor
@Slf4j
@Tag(name = "Traffic", description = "Network traffic statistics API")
public class TrafficController {

    private final TrafficQueryService trafficService;

    @GetMapping("/{srcIp}")
    @Operation(
            summary = "Get traffic for specific source IP",
            description = "Retrieves traffic aggregates for a given source IP within a time range"
    )
    public ResponseEntity<ApiResponseDto<Page<TrafficResponseDto>>> getTrafficByIp(
            @Parameter(description = "Source IP address", required = true)
            @PathVariable @ValidIpAddress String srcIp,

            @Parameter(description = "Time range parameters")
            @Valid TimeRangeRequestDto timeRange,

            @Parameter(description = "Pagination parameters")
            @Valid PaginationRequestDto pagination
    ) {
        log.debug("Traffic query for IP: {}, time range: {} to {}, page: {}, size: {}",
                srcIp, timeRange.getStartTime(), timeRange.getEndTime(),
                pagination.getPage(), pagination.getSize());

        TrafficQueryRequestDto request = TrafficQueryRequestDto.builder()
                .srcIp(srcIp)
                .startTime(timeRange.getStartTime())
                .endTime(timeRange.getEndTime())
                .pagination(pagination)
                .build();

        Page<TrafficResponseDto> result = trafficService.queryTrafficBySrcIp(srcIp, request);

        return ResponseEntity.ok(
                ApiResponseDto.success(result.getContent(), PaginationMetadataDto.of(
                        result.getTotalElements(),
                        result.getNumber(),
                        result.getSize()
                ))
        );
    }

    @GetMapping
    @Operation(
            summary = "List traffic with filters",
            description = "Retrieves traffic aggregates with optional filters and pagination"
    )
    public ResponseEntity<ApiResponseDto<Page<TrafficResponseDto>>> queryTraffic(
            @Parameter(description = "Source IP filter")
            @ValidIpAddress String srcIp,

            @Parameter(description = "Destination IP filter")
            @ValidIpAddress String dstIp,

            @Parameter(description = "Time range parameters")
            @Valid TimeRangeRequestDto timeRange,

            @Parameter(description = "Pagination parameters")
            @Valid PaginationRequestDto pagination
    ) {
        log.debug("Traffic query with filters - srcIp: {}, dstIp: {}, page: {}, size: {}",
                srcIp, dstIp, pagination.getPage(), pagination.getSize());

        TrafficQueryRequestDto request = TrafficQueryRequestDto.builder()
                .srcIp(srcIp)
                .dstIp(dstIp)
                .startTime(timeRange.getStartTime())
                .endTime(timeRange.getEndTime())
                .pagination(pagination)
                .build();

        Page<TrafficResponseDto> result = trafficService.queryTraffic(request);

        return ResponseEntity.ok(
                ApiResponseDto.success(result.getContent(), PaginationMetadataDto.of(
                        result.getTotalElements(),
                        result.getNumber(),
                        result.getSize()
                ))
        );
    }
}