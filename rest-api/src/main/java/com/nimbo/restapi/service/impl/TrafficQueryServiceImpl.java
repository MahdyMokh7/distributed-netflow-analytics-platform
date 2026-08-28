package com.nimbo.restapi.service.impl;

import com.nimbo.restapi.dto.request.TrafficQueryRequestDto;
import com.nimbo.restapi.dto.response.TrafficResponseDto;
import com.nimbo.restapi.exception.InvalidTimeRangeException;
import com.nimbo.restapi.exception.ResourceNotFoundException;
import com.nimbo.restapi.model.TrafficAggregate;
import com.nimbo.restapi.repository.TrafficRepository;
import com.nimbo.restapi.service.TrafficQueryService;
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
public class TrafficQueryServiceImpl implements TrafficQueryService {

    private final TrafficRepository trafficRepository;
    private final QueryMetrics metrics;

    private static final int MAX_TIME_RANGE_DAYS = 7;

    @Override
    @Cacheable(value = "traffic", key = "#request.srcIp + '_' + #request.startTime + '_' + #request.endTime + '_' + #request.pagination.page + '_' + #request.pagination.size")
    @CircuitBreaker(name = "database", fallbackMethod = "fallbackTrafficQuery")
    @Retry(name = "database")
    public Page<TrafficResponseDto> queryTraffic(TrafficQueryRequestDto request) {
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

            Page<TrafficAggregate> aggregates;

            if (request.getSrcIp() != null) {
                aggregates = trafficRepository.findBySrcIpAndTimeRange(
                        request.getSrcIp(),
                        request.getStartTime(),
                        request.getEndTime(),
                        pageable
                );
            } else {
                aggregates = trafficRepository.findByTimeRange(
                        request.getStartTime(),
                        request.getEndTime(),
                        pageable
                );
            }

            if (aggregates.isEmpty()) {
                throw new ResourceNotFoundException("No traffic data found for the given criteria");
            }

            Page<TrafficResponseDto> response = aggregates.map(this::toResponse);

            long duration = System.currentTimeMillis() - startTime;
            metrics.recordQuery("traffic", duration);

            return response;

        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            metrics.recordError("traffic", duration);
            throw e;
        }
    }

    @Override
    @Cacheable(value = "traffic", key = "#srcIp + '_' + #request.startTime + '_' + #request.endTime + '_' + #request.pagination.page + '_' + #request.pagination.size")
    @CircuitBreaker(name = "database", fallbackMethod = "fallbackTrafficQueryByIp")
    @Retry(name = "database")
    public Page<TrafficResponseDto> queryTrafficBySrcIp(String srcIp, TrafficQueryRequestDto request) {
        request.setSrcIp(srcIp);
        return queryTraffic(request);
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

    private TrafficResponseDto toResponse(TrafficAggregate aggregate) {
        return TrafficResponseDto.builder()
                .srcIp(aggregate.getSrcIp())
                .windowStart(aggregate.getWindowStart())
                .windowEnd(aggregate.getWindowEnd())
                .inBytes(aggregate.getInBytes())
                .outBytes(aggregate.getOutBytes())
                .totalBytes(aggregate.getInBytes().add(aggregate.getOutBytes()))
                .inPackets(aggregate.getInPackets())
                .outPackets(aggregate.getOutPackets())
                .totalPackets(aggregate.getInPackets().add(aggregate.getOutPackets()))
                .recordCount(aggregate.getRecordCount())
                .batchId(aggregate.getBatchId())
                .ingestedAt(aggregate.getIngestedAt())
                .build();
    }

    private Page<TrafficResponseDto> fallbackTrafficQuery(TrafficQueryRequestDto request, Throwable t) {
        log.warn("Circuit breaker fallback triggered for traffic query: {}", t.getMessage());
        return Page.empty();
    }

    private Page<TrafficResponseDto> fallbackTrafficQueryByIp(String srcIp, TrafficQueryRequestDto request, Throwable t) {
        log.warn("Circuit breaker fallback triggered for traffic query by IP: {}", t.getMessage());
        return Page.empty();
    }
}