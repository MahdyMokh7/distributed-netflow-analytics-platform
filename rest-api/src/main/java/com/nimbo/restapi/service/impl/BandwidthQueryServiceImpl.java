package com.nimbo.restapi.service.impl;

import com.nimbo.restapi.dto.request.BandwidthQueryRequestDto;
import com.nimbo.restapi.dto.response.BandwidthResponseDto;
import com.nimbo.restapi.exception.InvalidTimeRangeException;
import com.nimbo.restapi.exception.ResourceNotFoundException;
import com.nimbo.restapi.model.IpBandwidthAggregate;
import com.nimbo.restapi.repository.BandwidthRepository;
import com.nimbo.restapi.service.BandwidthQueryService;
import com.nimbo.restapi.util.QueryMetrics;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
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
public class BandwidthQueryServiceImpl implements BandwidthQueryService {

    private final BandwidthRepository bandwidthRepository;
    private final QueryMetrics metrics;

    private static final int MAX_TIME_RANGE_DAYS = 7;
    private static final int DEFAULT_TOP_LIMIT = 10;
    private static final int MAX_TOP_LIMIT = 100;

    @Override
    @Cacheable(value = "bandwidth", key = "'top_' + #request.limit + '_' + #request.startTime + '_' + #request.endTime")
    @CircuitBreaker(name = "database", fallbackMethod = "fallbackTopBandwidth")
    @Retry(name = "database")
    public List<BandwidthResponseDto> getTopBandwidthIps(BandwidthQueryRequestDto request) {
        long startTime = System.currentTimeMillis();

        try {
            validateTimeRange(request.getStartTime(), request.getEndTime());

            int limit = request.getLimit() != null ?
                    Math.min(request.getLimit(), MAX_TOP_LIMIT) :
                    DEFAULT_TOP_LIMIT;

            Pageable pageable = PageRequest.of(0, limit);
            Page<Object[]> topBandwidth = bandwidthRepository.findTopBandwidthIps(
                    request.getStartTime(),
                    request.getEndTime(),
                    pageable
            );

            if (topBandwidth.isEmpty()) {
                throw new ResourceNotFoundException("No bandwidth data found for the given criteria");
            }

            List<BandwidthResponseDto> responses = topBandwidth.getContent().stream()
                    .map(this::toTopResponse)
                    .collect(Collectors.toList());

            long duration = System.currentTimeMillis() - startTime;
            metrics.recordQuery("bandwidth_top", duration);

            return responses;

        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            metrics.recordError("bandwidth_top", duration);
            throw e;
        }
    }

    @Override
    @Cacheable(value = "bandwidth", key = "#ip + '_' + #request.startTime + '_' + #request.endTime")
    @CircuitBreaker(name = "database", fallbackMethod = "fallbackBandwidthByIp")
    @Retry(name = "database")
    public BandwidthResponseDto queryBandwidthByIp(String ip, BandwidthQueryRequestDto request) {
        long startTime = System.currentTimeMillis();

        try {
            validateTimeRange(request.getStartTime(), request.getEndTime());

            List<IpBandwidthAggregate> aggregates = bandwidthRepository.findByIpAndTimeRange(
                    ip,
                    request.getStartTime(),
                    request.getEndTime()
            );

            if (aggregates.isEmpty()) {
                throw new ResourceNotFoundException("No bandwidth data found for IP: " + ip);
            }

            IpBandwidthAggregate aggregate = aggregates.get(0);

            long duration = System.currentTimeMillis() - startTime;
            metrics.recordQuery("bandwidth_by_ip", duration);

            return toResponse(aggregate);

        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;
            metrics.recordError("bandwidth_by_ip", duration);
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

    private BandwidthResponseDto toTopResponse(Object[] row) {
        return BandwidthResponseDto.builder()
                .ip((String) row[0])
                .totalBytes((java.math.BigInteger) row[4])
                .build();
    }

    private BandwidthResponseDto toResponse(IpBandwidthAggregate aggregate) {
        return BandwidthResponseDto.builder()
                .ip(aggregate.getIp())
                .windowStart(aggregate.getWindowStart())
                .windowEnd(aggregate.getWindowEnd())
                .inboundBytes(aggregate.getInboundBytes())
                .outboundBytes(aggregate.getOutboundBytes())
                .totalBytes(aggregate.getInboundBytes().add(aggregate.getOutboundBytes()))
                .inboundPackets(aggregate.getInboundPackets())
                .outboundPackets(aggregate.getOutboundPackets())
                .totalPackets(aggregate.getInboundPackets().add(aggregate.getOutboundPackets()))
                .batchId(aggregate.getBatchId())
                .ingestedAt(aggregate.getIngestedAt())
                .build();
    }

    private List<BandwidthResponseDto> fallbackTopBandwidth(BandwidthQueryRequestDto request, Throwable t) {
        log.warn("Circuit breaker fallback triggered for top bandwidth query: {}", t.getMessage());
        return List.of();
    }

    private BandwidthResponseDto fallbackBandwidthByIp(String ip, BandwidthQueryRequestDto request, Throwable t) {
        log.warn("Circuit breaker fallback triggered for bandwidth by IP query: {}", t.getMessage());
        return BandwidthResponseDto.builder().build();
    }
}