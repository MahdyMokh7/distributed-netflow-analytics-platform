package com.nimbo.restapi.service.impl;

import com.nimbo.restapi.dto.request.BandwidthQueryRequestDto;
import com.nimbo.restapi.dto.response.BandwidthResponseDto;
import com.nimbo.restapi.dto.response.TopBandwidthResponseDto;
import com.nimbo.restapi.exception.InvalidTimeRangeException;
import com.nimbo.restapi.exception.ResourceNotFoundException;
import com.nimbo.restapi.repository.BandwidthRepository;
import com.nimbo.restapi.repository.projection.BandwidthSummaryProjection;
import com.nimbo.restapi.repository.projection.BandwidthWindowProjection;
import com.nimbo.restapi.service.BandwidthQueryService;
import com.nimbo.restapi.util.QueryMetrics;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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
  @Cacheable(
      value = "bandwidth",
      key = "'top_' + #request.limit + '_' + #request.startTime + '_' + #request.endTime")
  @CircuitBreaker(name = "database")
  @Retry(name = "database")
  public List<TopBandwidthResponseDto> getTopBandwidthIps(BandwidthQueryRequestDto request) {
    long startTime = System.currentTimeMillis();

    try {
      validateTimeRange(request.getStartTime(), request.getEndTime());

      int limit =
          request.getLimit() != null
              ? Math.min(request.getLimit(), MAX_TOP_LIMIT)
              : DEFAULT_TOP_LIMIT;

      Pageable pageable = PageRequest.of(0, limit);
      List<BandwidthSummaryProjection> topBandwidth =
          bandwidthRepository.findTopBandwidthIps(
              request.getStartTime(), request.getEndTime(), pageable);

      if (topBandwidth.isEmpty()) {
        throw new ResourceNotFoundException("No bandwidth data found for the given criteria");
      }

      List<TopBandwidthResponseDto> responses =
          topBandwidth.stream().map(this::toTopResponse).collect(Collectors.toList());

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
  @CircuitBreaker(name = "database")
  @Retry(name = "database")
  public BandwidthResponseDto queryBandwidthByIp(String ip, BandwidthQueryRequestDto request) {
    long startTime = System.currentTimeMillis();

    try {
      validateTimeRange(request.getStartTime(), request.getEndTime());

      List<BandwidthWindowProjection> aggregates =
          bandwidthRepository.findByIpAndTimeRange(
              ip, request.getStartTime(), request.getEndTime());

      if (aggregates.isEmpty()) {
        throw new ResourceNotFoundException("No bandwidth data found for IP: " + ip);
      }

      BandwidthWindowProjection aggregate = aggregates.get(0);

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
    if (!end.isAfter(start)) {
      throw new InvalidTimeRangeException("End time must be after start time");
    }

    Instant now = Instant.now();
    if (end.isAfter(now)) {
      throw new InvalidTimeRangeException("Cannot query future dates");
    }

    if (Duration.between(start, end).compareTo(Duration.ofDays(MAX_TIME_RANGE_DAYS)) > 0) {
      throw new InvalidTimeRangeException(
          "Time range cannot exceed " + MAX_TIME_RANGE_DAYS + " days");
    }
  }

  private TopBandwidthResponseDto toTopResponse(BandwidthSummaryProjection row) {
    BigInteger inboundPackets = row.getInboundPackets();
    BigInteger outboundPackets = row.getOutboundPackets();
    return TopBandwidthResponseDto.builder()
        .ip(row.getIp())
        .inboundBytes(row.getInboundBytes())
        .outboundBytes(row.getOutboundBytes())
        .inboundPackets(inboundPackets)
        .outboundPackets(outboundPackets)
        .totalPackets(inboundPackets.add(outboundPackets))
        .totalBytes(row.getTotalBytes())
        .build();
  }

  private BandwidthResponseDto toResponse(BandwidthWindowProjection aggregate) {
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
        .ingestedAt(aggregate.getIngestedAt())
        .build();
  }
}
