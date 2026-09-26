package com.nimbo.restapi.controller;

import com.nimbo.restapi.dto.request.ConnectionQueryRequestDto;
import com.nimbo.restapi.dto.request.PaginationRequestDto;
import com.nimbo.restapi.dto.request.TimeRangeRequestDto;
import com.nimbo.restapi.dto.response.ApiResponseDto;
import com.nimbo.restapi.dto.response.ConnectionResponseDto;
import com.nimbo.restapi.dto.response.PaginationMetadataDto;
import com.nimbo.restapi.service.ConnectionQueryService;
import com.nimbo.restapi.validation.ValidIpAddress;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
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

import java.util.List;

@RestController
@RequestMapping("/connections")
@RequiredArgsConstructor
@Slf4j
@Tag(name = "Connections", description = "Network connection statistics API")
public class ConnectionController {

    private final ConnectionQueryService connectionService;

    @GetMapping("/{ip}")
    @Operation(
            summary = "Get connections for an IP",
            description = "Retrieves connection aggregates for a given IP (as source or destination)"
    )
    @Parameters({
            @Parameter(name = "startTime", description = "Start of the query time range", required = true,
                    in = ParameterIn.QUERY, example = "2026-09-07T04:47:15.772Z"),
            @Parameter(name = "endTime", description = "End of the query time range", required = true,
                    in = ParameterIn.QUERY, example = "2026-09-09T04:47:15.772Z"),
            @Parameter(name = "page", description = "Zero-based page number", required = true,
                    in = ParameterIn.QUERY, example = "0"),
            @Parameter(name = "size", description = "Page size (1-100)", required = true,
                    in = ParameterIn.QUERY, example = "10"),
            @Parameter(name = "sortBy", description = "Response field used for sorting",
                    in = ParameterIn.QUERY, example = "inBytes"),
            @Parameter(name = "sortDirection", description = "Sort direction (ASC or DESC)",
                    in = ParameterIn.QUERY, example = "DESC")
    })
    public ResponseEntity<ApiResponseDto<List<ConnectionResponseDto>>> getConnectionsByIp(
            @Parameter(
                    description = "IP address (source or destination)",
                    required = true,
                    example = "172.31.69.23")
            @PathVariable @ValidIpAddress String ip,

            @Parameter(hidden = true)
            @Valid TimeRangeRequestDto timeRange,

            @Parameter(hidden = true)
            @Valid PaginationRequestDto pagination
    ) {
        log.debug("Connection query for IP: {}, time range: {} to {}, page: {}, size: {}",
                ip, timeRange.getStartTime(), timeRange.getEndTime(),
                pagination.getPage(), pagination.getSize());

        ConnectionQueryRequestDto request = ConnectionQueryRequestDto.builder()
                .startTime(timeRange.getStartTime())
                .endTime(timeRange.getEndTime())
                .pagination(pagination)
                .build();

        Page<ConnectionResponseDto> result = connectionService.queryConnectionsByIp(ip, request);

        return ResponseEntity.ok(
                ApiResponseDto.success(result.getContent(), PaginationMetadataDto.of(result))
        );
    }

    @GetMapping
    @Operation(
            summary = "List connections with filters",
            description = "Retrieves connection aggregates with optional filters and pagination"
    )
    @Parameters({
            @Parameter(name = "startTime", description = "Start of the query time range", required = true,
                    in = ParameterIn.QUERY, example = "2026-09-07T04:47:15.772Z"),
            @Parameter(name = "endTime", description = "End of the query time range", required = true,
                    in = ParameterIn.QUERY, example = "2026-09-09T04:47:15.772Z"),
            @Parameter(name = "page", description = "Zero-based page number", required = true,
                    in = ParameterIn.QUERY, example = "0"),
            @Parameter(name = "size", description = "Page size (1-100)", required = true,
                    in = ParameterIn.QUERY, example = "10"),
            @Parameter(name = "sortBy", description = "Response field used for sorting",
                    in = ParameterIn.QUERY, example = "inBytes"),
            @Parameter(name = "sortDirection", description = "Sort direction (ASC or DESC)",
                    in = ParameterIn.QUERY, example = "DESC")
    })
    public ResponseEntity<ApiResponseDto<List<ConnectionResponseDto>>> queryConnections(
            @Parameter(description = "Optional source IP filter", example = "172.31.69.23")
            @ValidIpAddress
            String srcIp,

            @Parameter(description = "Optional destination IP filter", example = "18.219.211.138")
            @ValidIpAddress
            String dstIp,

            @Parameter(hidden = true)
            @Valid
            TimeRangeRequestDto timeRange,

            @Parameter(hidden = true)
            @Valid
            PaginationRequestDto pagination
    ) {
        log.debug("Connection query with filters - srcIp: {}, dstIp: {}", srcIp, dstIp);

        ConnectionQueryRequestDto request = ConnectionQueryRequestDto.builder()
                .srcIp(srcIp)
                .dstIp(dstIp)
                .startTime(timeRange.getStartTime())
                .endTime(timeRange.getEndTime())
                .pagination(pagination)
                .build();

        Page<ConnectionResponseDto> result = connectionService.queryConnections(request);

        return ResponseEntity.ok(
                ApiResponseDto.success(result.getContent(), PaginationMetadataDto.of(result))
        );
    }
}
