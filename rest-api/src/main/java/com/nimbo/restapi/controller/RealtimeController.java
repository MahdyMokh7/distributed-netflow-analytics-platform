package com.nimbo.restapi.controller;

import com.nimbo.restapi.dto.request.TimeRangeRequestDto;
import com.nimbo.restapi.dto.response.ApiResponseDto;
import com.nimbo.restapi.dto.response.PaginationMetadataDto;
import com.nimbo.restapi.dto.response.realtime.RealtimeBandwidthResponseDto;
import com.nimbo.restapi.dto.response.realtime.RealtimeConnectionResponseDto;
import com.nimbo.restapi.dto.response.realtime.RealtimeProtocolResponseDto;
import com.nimbo.restapi.dto.response.realtime.RealtimeTrafficResponseDto;
import com.nimbo.restapi.service.RealtimeQueryService;
import com.nimbo.restapi.validation.ValidIpAddress;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/realtime")
@RequiredArgsConstructor
@Validated
@Tag(name = "Realtime Analytics", description = "Realtime NetFlow queries backed by StarRocks")
public class RealtimeController {

    private final RealtimeQueryService queryService;

    @GetMapping("/traffic/{ip}")
    @Operation(summary = "Get traffic for an IP")
    public ResponseEntity<ApiResponseDto<RealtimeTrafficResponseDto>> getTrafficByIp(
            @PathVariable
            @ValidIpAddress
            String ip,

            @Valid
            TimeRangeRequestDto timeRange
    ) {
        RealtimeTrafficResponseDto result =
                queryService.getTrafficByIp(ip, timeRange.getStartTime(), timeRange.getEndTime());

        return ResponseEntity.ok(ApiResponseDto.success(result));
    }

    @GetMapping("/connections/{ip}")
    @Operation(summary = "List connections for an IP")
    public ResponseEntity<ApiResponseDto<List<RealtimeConnectionResponseDto>>> getConnectionsByIp(

            @PathVariable
            @ValidIpAddress
            String ip,

            @Valid
            TimeRangeRequestDto timeRange,

            @RequestParam(defaultValue = "0")
            int page,

            @RequestParam(defaultValue = "20")
            int size
    ) {
        Page<RealtimeConnectionResponseDto> result =
                queryService.getConnectionsByIp(ip, timeRange.getStartTime(), timeRange.getEndTime(), page, size);

        PaginationMetadataDto pagination =
                PaginationMetadataDto.of(result.getTotalElements(), result.getNumber(), result.getSize());

        return ResponseEntity.ok(
                ApiResponseDto.success(result.getContent(), pagination));
    }

    @GetMapping("/bandwidth/top")
    @Operation(summary = "Get IPs with highest bandwidth usage")
    public ResponseEntity<ApiResponseDto<List<RealtimeBandwidthResponseDto>>> getTopBandwidthIps(

            @Valid
            TimeRangeRequestDto timeRange,

            @Parameter(description = "Maximum number of IPs")
            @RequestParam(defaultValue = "10")
            int limit
    ) {
        List<RealtimeBandwidthResponseDto> result =
                queryService.getTopBandwidthIps(timeRange.getStartTime(), timeRange.getEndTime(), limit);

        return ResponseEntity.ok(ApiResponseDto.success(result));
    }

    @GetMapping("/protocols")
    @Operation(summary = "Get most commonly used protocols")
    public ResponseEntity<ApiResponseDto<List<RealtimeProtocolResponseDto>>> getProtocolUsage(

            @Valid
            TimeRangeRequestDto timeRange
    ) {
        List<RealtimeProtocolResponseDto> result =
                queryService.getProtocolUsage(timeRange.getStartTime(), timeRange.getEndTime());

        return ResponseEntity.ok(ApiResponseDto.success(result));
    }
}