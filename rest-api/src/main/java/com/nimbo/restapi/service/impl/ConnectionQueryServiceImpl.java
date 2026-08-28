package com.nimbo.restapi.service.impl;

import com.nimbo.restapi.dto.request.ConnectionQueryRequestDto;
import com.nimbo.restapi.dto.response.ConnectionResponseDto;
import com.nimbo.restapi.exception.InvalidTimeRangeException;
import com.nimbo.restapi.exception.ResourceNotFoundException;
import com.nimbo.restapi.model.ConnectionAggregate;
import com.nimbo.restapi.repository.ConnectionRepository;
import com.nimbo.restapi.service.ConnectionQueryService;
import com.nimbo.restapi.util.QueryMetrics;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

@Service
@RequiredArgsConstructor
@Slf4j
@Transactional(readOnly = true)
public class ConnectionQueryServiceImpl implements ConnectionQueryService {

    private final ConnectionRepository connectionRepository;
    private final QueryMetrics metrics;

    private static final int MAX_TIME_RANGE_DAYS = 7;

    @Override
    @Cacheable(value = "connections", key = "#request.srcIp + '_' + #request.dstIp + '_' + #request.srcPort + '_' + #request.dstPort + '_' + #request.l4Protocol + '_' + #request.startTime + '_' + #request.endTime + '_' + #request.pagination.page + '_' + #request.pagination.size")
    @CircuitBreaker(name = "database", fallbackMethod = "fallbackConnectionQuery")
    @Retry(name = "database")
    public Page<ConnectionResponseDto> queryConnections(ConnectionQueryRequestDto request) {
        long startTime = System.currentTimeMillis();

        try {
            validateTimeRange(request.getStartTime(), request.getEndTime());

            Pageable pageable = PageRequest.of(
                    request.getPagination().getPage(),
                    request.getPagination().getSize(),
                    Sort.by(Sort.Direction.fromString(
                                    request.getPagination().getSortDirection()),
                            request.getPagination().getSortBy())
            );

            Page<ConnectionAggregate> aggregates;

            if (request.getSrcIp() != null && request.getDstIp() != null) {
                aggregates = connectionRepository.findBySrcIpAndDstIpAndTimeRange(
                        request.getSrcIp(),
                        request.getDstIp(),
                        request.getStartTime(),
                        request.getEndTime(),
                        pageable
                );
            } else if (request.getSrcIp() != null) {
                aggregates = connectionRepository.findBySrcIpAndTimeRange(
                        request.getSrcIp(),
                        request.getStartTime(),
                        request.getEndTime(),
                        pageable
                );
            } else if (request.getDstIp() != null) {
                aggregates = connectionRepository.findByDstIpAndTimeRange(
                        request.getDstIp(),
                        request.getStartTime(),
                        request.getEndTime(),
                        pageable
                );
            } else {
                aggregates = connectionRepository.findByTimeRange(
                        request.getStartTime(),
                        request.getEndTime(),
                        pageable
                );
            }

            if (aggregates.isEmpty()) {
                throw new ResourceNotFoundException("No connection data found for the given criteria");
            }

            Page<ConnectionResponseDto> response = aggregates.map(this::toResponse);

            long duration = System.currentTimeMillis() - startTime;
            metrics.recordQuery("connections", duration);

            return response;

        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            metrics.recordError("connections", duration);
            throw e;
        }
    }

    @Override
    @Cacheable(value = "connections", key = "#ip + '_' + #request.startTime + '_' + #request.endTime + '_' + #request.pagination.page + '_' + #request.pagination.size")
    @CircuitBreaker(name = "database", fallbackMethod = "fallbackConnectionQueryByIp")
    @Retry(name = "database")
    public Page<ConnectionResponseDto> queryConnectionsByIp(String ip, ConnectionQueryRequestDto request) {
        long startTime = System.currentTimeMillis();

        try {
            validateTimeRange(request.getStartTime(), request.getEndTime());

            Pageable pageable = PageRequest.of(
                    request.getPagination().getPage(),
                    request.getPagination().getSize(),
                    Sort.by(Sort.Direction.fromString(
                                    request.getPagination().getSortDirection()),
                            request.getPagination().getSortBy())
            );

            Page<ConnectionAggregate> aggregates = connectionRepository.findByIpAndTimeRange(
                    ip,
                    request.getStartTime(),
                    request.getEndTime(),
                    pageable
            );

            if (aggregates.isEmpty()) {
                throw new ResourceNotFoundException("No connection data found for IP: " + ip);
            }

            Page<ConnectionResponseDto> response = aggregates.map(this::toResponse);

            long duration = System.currentTimeMillis() - startTime;
            metrics.recordQuery("connections_by_ip", duration);

            return response;

        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            metrics.recordError("connections_by_ip", duration);
            throw e;
        }
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

    private ConnectionResponseDto toResponse(ConnectionAggregate aggregate) {
        return ConnectionResponseDto.builder()
                .id(aggregate.getId())
                .srcIp(aggregate.getSrcIp())
                .dstIp(aggregate.getDstIp())
                .srcPort(aggregate.getSrcPort())
                .dstPort(aggregate.getDstPort())
                .l4Protocol(aggregate.getL4Protocol())
                .protocolName(aggregate.getProtocolName())
                .windowStart(aggregate.getWindowStart())
                .windowEnd(aggregate.getWindowEnd())
                .inBytes(aggregate.getInBytes())
                .outBytes(aggregate.getOutBytes())
                .totalBytes(aggregate.getInBytes().add(aggregate.getOutBytes()))
                .inPackets(aggregate.getInPackets())
                .outPackets(aggregate.getOutPackets())
                .totalPackets(aggregate.getInPackets().add(aggregate.getOutPackets()))
                .batchId(aggregate.getBatchId())
                .ingestedAt(aggregate.getIngestedAt())
                .build();
    }

    private Page<ConnectionResponseDto> fallbackConnectionQuery(ConnectionQueryRequestDto request, Throwable t) {
        log.warn("Circuit breaker fallback triggered for connection query: {}", t.getMessage());
        return Page.empty();
    }

    private Page<ConnectionResponseDto> fallbackConnectionQueryByIp(String ip, ConnectionQueryRequestDto request, Throwable t) {
        log.warn("Circuit breaker fallback triggered for connection query by IP: {}", t.getMessage());
        return Page.empty();
    }
}