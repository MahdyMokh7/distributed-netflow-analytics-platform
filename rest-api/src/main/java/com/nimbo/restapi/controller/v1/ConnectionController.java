package com.nimbo.restapi.controller.v1;

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
@RequestMapping("/v1/connections")
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
    public ResponseEntity<ApiResponseDto<List<ConnectionResponseDto>>> getConnectionsByIp(
            @Parameter(description = "IP address (source or destination)", required = true)
            @PathVariable @ValidIpAddress String ip,

            @Parameter(description = "Time range parameters")
            @Valid TimeRangeRequestDto timeRange,

            @Parameter(description = "Pagination parameters")
            @Valid PaginationRequestDto pagination
    ) {
        log.debug("Connection query for IP: {}, time range: {} to {}, page: {}, size: {}",
                ip, timeRange.getStartTime(), timeRange.getEndTime(),
                pagination.getPage(), pagination.getSize());

        ConnectionQueryRequestDto request = ConnectionQueryRequestDto.builder()
                .srcIp(ip)
                .dstIp(ip)
                .startTime(timeRange.getStartTime())
                .endTime(timeRange.getEndTime())
                .pagination(pagination)
                .build();

        Page<ConnectionResponseDto> result = connectionService.queryConnectionsByIp(ip, request);

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
            summary = "List connections with filters",
            description = "Retrieves connection aggregates with optional filters and pagination"
    )
    public ResponseEntity<ApiResponseDto<List<ConnectionResponseDto>>> queryConnections(
            @Parameter(description = "Source IP filter")
            @ValidIpAddress String srcIp,

            @Parameter(description = "Destination IP filter")
            @ValidIpAddress String dstIp,

            @Parameter(description = "Source port filter")
            Integer srcPort,

            @Parameter(description = "Destination port filter")
            Integer dstPort,

            @Parameter(description = "Layer 4 protocol filter")
            Integer l4Protocol,

            @Parameter(description = "Time range parameters")
            @Valid TimeRangeRequestDto timeRange,

            @Parameter(description = "Pagination parameters")
            @Valid PaginationRequestDto pagination
    ) {
        log.debug("Connection query with filters - srcIp: {}, dstIp: {}, srcPort: {}, dstPort: {}, l4Protocol: {}",
                srcIp, dstIp, srcPort, dstPort, l4Protocol);

        ConnectionQueryRequestDto request = ConnectionQueryRequestDto.builder()
                .srcIp(srcIp)
                .dstIp(dstIp)
                .srcPort(srcPort)
                .dstPort(dstPort)
                .l4Protocol(l4Protocol)
                .startTime(timeRange.getStartTime())
                .endTime(timeRange.getEndTime())
                .pagination(pagination)
                .build();

        Page<ConnectionResponseDto> result = connectionService.queryConnections(request);

        return ResponseEntity.ok(
                ApiResponseDto.success(result.getContent(), PaginationMetadataDto.of(
                        result.getTotalElements(),
                        result.getNumber(),
                        result.getSize()
                ))
        );
    }
}