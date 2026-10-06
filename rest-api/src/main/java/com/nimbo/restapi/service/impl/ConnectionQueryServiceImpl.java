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
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Slf4j
@Transactional(readOnly = true)
public class ConnectionQueryServiceImpl implements ConnectionQueryService {

  private final ConnectionRepository connectionRepository;
  private final QueryMetrics metrics;

  private static final int MAX_TIME_RANGE_DAYS = 7;
  private static final Set<String> ALLOWED_SORT_FIELDS =
      Set.of(
          "id",
          "srcIp",
          "dstIp",
          "windowStart",
          "windowEnd",
          "inBytes",
          "outBytes",
          "inPackets",
          "outPackets",
          "ingestedAt");

  @Override
  @Cacheable(
      value = "connections",
      key =
          "#request.srcIp + '_' + #request.dstIp + '_' + #request.startTime + '_' +"
              + " #request.endTime + '_' + #request.pagination.page + '_' +"
              + " #request.pagination.size + '_' + #request.pagination.sortBy + '_' +"
              + " #request.pagination.sortDirection")
  @CircuitBreaker(name = "database")
  @Retry(name = "database")
  public Page<ConnectionResponseDto> queryConnections(ConnectionQueryRequestDto request) {
    long startTime = System.currentTimeMillis();

    try {
      validateTimeRange(request.getStartTime(), request.getEndTime());

      Pageable pageable = createPageable(request);

      Page<ConnectionAggregate> aggregates;

      if (request.getSrcIp() != null && request.getDstIp() != null) {
        aggregates =
            connectionRepository.findBySrcIpAndDstIpAndTimeRange(
                request.getSrcIp(),
                request.getDstIp(),
                request.getStartTime(),
                request.getEndTime(),
                pageable);
      } else if (request.getSrcIp() != null) {
        aggregates =
            connectionRepository.findBySrcIpAndTimeRange(
                request.getSrcIp(), request.getStartTime(), request.getEndTime(), pageable);
      } else if (request.getDstIp() != null) {
        aggregates =
            connectionRepository.findByDstIpAndTimeRange(
                request.getDstIp(), request.getStartTime(), request.getEndTime(), pageable);
      } else {
        aggregates =
            connectionRepository.findByTimeRange(
                request.getStartTime(), request.getEndTime(), pageable);
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
  @Cacheable(
      value = "connections",
      key =
          "#ip + '_' + #request.startTime + '_' + #request.endTime + '_' + #request.pagination.page"
              + " + '_' + #request.pagination.size + '_' + #request.pagination.sortBy + '_' +"
              + " #request.pagination.sortDirection")
  @CircuitBreaker(name = "database")
  @Retry(name = "database")
  public Page<ConnectionResponseDto> queryConnectionsByIp(
      String ip, ConnectionQueryRequestDto request) {
    long startTime = System.currentTimeMillis();

    try {
      validateTimeRange(request.getStartTime(), request.getEndTime());

      Pageable pageable = createPageable(request);

      Page<ConnectionAggregate> aggregates =
          connectionRepository.findByIpAndTimeRange(
              ip, request.getStartTime(), request.getEndTime(), pageable);

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

  private ConnectionResponseDto toResponse(ConnectionAggregate aggregate) {
    return ConnectionResponseDto.builder()
        .id(HexFormat.of().formatHex(aggregate.getId()))
        .srcIp(aggregate.getSrcIp())
        .dstIp(aggregate.getDstIp())
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

  private Pageable createPageable(ConnectionQueryRequestDto request) {
    String sortBy = request.getPagination().getSortBy();
    if (!ALLOWED_SORT_FIELDS.contains(sortBy)) {
      throw new IllegalArgumentException("Unsupported connection sort field: " + sortBy);
    }
    return PageRequest.of(
        request.getPagination().getPage(),
        request.getPagination().getSize(),
        Sort.by(Sort.Direction.fromString(request.getPagination().getSortDirection()), sortBy));
  }
}
