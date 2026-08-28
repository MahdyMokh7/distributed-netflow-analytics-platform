package com.nimbo.restapi.service.impl;

import com.nimbo.restapi.dto.request.ProtocolQueryRequestDto;
import com.nimbo.restapi.dto.response.ProtocolResponseDto;
import com.nimbo.restapi.exception.InvalidTimeRangeException;
import com.nimbo.restapi.exception.ResourceNotFoundException;
import com.nimbo.restapi.model.ProtocolUsage;
import com.nimbo.restapi.repository.ProtocolRepository;
import com.nimbo.restapi.service.ProtocolQueryService;
import com.nimbo.restapi.util.QueryMetrics;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
@Transactional(readOnly = true)
public class ProtocolQueryServiceImpl implements ProtocolQueryService {

    private final ProtocolRepository protocolRepository;
    private final QueryMetrics metrics;

    private static final int MAX_TIME_RANGE_DAYS = 7;

    @Override
    @Cacheable(value = "protocols", key = "#request.startTime + '_' + #request.endTime + '_' + #request.l7Protocol")
    @CircuitBreaker(name = "database", fallbackMethod = "fallbackProtocolQuery")
    @Retry(name = "database")
    public List<ProtocolResponseDto> queryProtocolUsage(ProtocolQueryRequestDto request) {
        long startTime = System.currentTimeMillis();

        try {
            validateTimeRange(request.getStartTime(), request.getEndTime());

            List<ProtocolUsage> usages;

            if (request.getL7Protocol() != null) {
                usages = protocolRepository.findByProtocolAndTimeRange(
                        request.getL7Protocol(),
                        request.getStartTime(),
                        request.getEndTime()
                );
            } else {
                usages = protocolRepository.findByTimeRange(
                        request.getStartTime(),
                        request.getEndTime()
                );
            }

            if (usages.isEmpty()) {
                throw new ResourceNotFoundException("No protocol usage data found for the given criteria");
            }

            List<ProtocolResponseDto> responses = usages.stream()
                    .map(this::toResponse)
                    .collect(Collectors.toList());

            long duration = System.currentTimeMillis() - startTime;
            metrics.recordQuery("protocols", duration);

            return responses;

        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            metrics.recordError("protocols", duration);
            throw e;
        }
    }

    @Override
    @Cacheable(value = "protocols", key = "#l7Protocol + '_' + #request.startTime + '_' + #request.endTime")
    @CircuitBreaker(name = "database", fallbackMethod = "fallbackProtocolQueryByProtocol")
    @Retry(name = "database")
    public ProtocolResponseDto queryProtocolUsageByProtocol(Double l7Protocol, ProtocolQueryRequestDto request) {
        request.setL7Protocol(l7Protocol);
        List<ProtocolResponseDto> results = queryProtocolUsage(request);
        if (results.isEmpty()) {
            throw new ResourceNotFoundException("No protocol usage found for protocol: " + l7Protocol);
        }
        return results.get(0);
    }

    private void validateTimeRange(Instant start, Instant end) {
        if (end.isBefore(start)) {
            throw new InvalidTimeRangeException("End time must be after start time");
        }

        Instant now = Instant.now();
        if (end.isAfter(now)) {
            throw new InvalidTimeRangeException("Cannot query future dates");
        }

        long daysBetween = ChronoUnit.DAYS.between(start, end);
        if (daysBetween > MAX_TIME_RANGE_DAYS) {
            throw new InvalidTimeRangeException(
                    "Time range cannot exceed " + MAX_TIME_RANGE_DAYS + " days"
            );
        }
    }

    private ProtocolResponseDto toResponse(ProtocolUsage usage) {
        return ProtocolResponseDto.builder()
                .l7Protocol(usage.getL7Protocol())
                .protocolName(usage.getProtocolName())
                .windowStart(usage.getWindowStart())
                .windowEnd(usage.getWindowEnd())
                .inBytes(usage.getInBytes())
                .outBytes(usage.getOutBytes())
                .totalBytes(usage.getInBytes().add(usage.getOutBytes()))
                .inPackets(usage.getInPackets())
                .outPackets(usage.getOutPackets())
                .totalPackets(usage.getInPackets().add(usage.getOutPackets()))
                .connectionCount(usage.getConnectionCount())
                .batchId(usage.getBatchId())
                .ingestedAt(usage.getIngestedAt())
                .build();
    }

    private List<ProtocolResponseDto> fallbackProtocolQuery(ProtocolQueryRequestDto request, Throwable t) {
        log.warn("Circuit breaker fallback triggered for protocol query: {}", t.getMessage());
        return List.of();
    }

    private ProtocolResponseDto fallbackProtocolQueryByProtocol(Double l7Protocol, ProtocolQueryRequestDto request, Throwable t) {
        log.warn("Circuit breaker fallback triggered for protocol query by protocol: {}", t.getMessage());
        return ProtocolResponseDto.builder().build();
    }
}