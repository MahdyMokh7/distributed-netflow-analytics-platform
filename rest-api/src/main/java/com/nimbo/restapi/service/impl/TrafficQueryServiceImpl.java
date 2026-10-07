package com.nimbo.restapi.service.impl;

import com.nimbo.restapi.dto.request.TrafficQueryRequestDto;
import com.nimbo.restapi.dto.response.TrafficResponseDto;
import com.nimbo.restapi.exception.InvalidTimeRangeException;
import com.nimbo.restapi.exception.ResourceNotFoundException;
import com.nimbo.restapi.repository.TrafficRepository;
import com.nimbo.restapi.repository.projection.TrafficWindowProjection;
import com.nimbo.restapi.service.TrafficQueryService;
import com.nimbo.restapi.util.QueryMetrics;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import java.time.Duration;
import java.time.Instant;
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
public class TrafficQueryServiceImpl implements TrafficQueryService {

  private final TrafficRepository trafficRepository;
  private final QueryMetrics metrics;

  private static final int MAX_TIME_RANGE_DAYS = 7;
  private static final Set<String> ALLOWED_SORT_FIELDS =
      Set.of(
          "srcIp",
          "windowStart",
          "windowEnd",
          "inBytes",
          "outBytes",
          "inPackets",
          "outPackets",
          "recordCount",
          "ingestedAt");

  @Override
  @Cacheable(
      value = "traffic",
      key =
          "#request.srcIp + '_' + #request.startTime + '_' + #request.endTime + '_' +"
              + " #request.pagination.page + '_' + #request.pagination.size + '_' +"
              + " #request.pagination.sortBy + '_' + #request.pagination.sortDirection")
  @CircuitBreaker(name = "database")
  @Retry(name = "database")
  public Page<TrafficResponseDto> queryTraffic(TrafficQueryRequestDto request) {
    long startTime = System.currentTimeMillis();

    try {
      validateTimeRange(request.getStartTime(), request.getEndTime());

      Pageable pageable = createPageable(request);

      Page<TrafficWindowProjection> aggregates;

      if (request.getSrcIp() != null) {
        aggregates =
            trafficRepository.findBySrcIpAndTimeRange(
                request.getSrcIp(), request.getStartTime(), request.getEndTime(), pageable);
      } else {
        aggregates =
            trafficRepository.findByTimeRange(
                request.getStartTime(), request.getEndTime(), pageable);
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
  @Cacheable(
      value = "traffic",
      key =
          "#srcIp + '_' + #request.startTime + '_' + #request.endTime + '_' +"
              + " #request.pagination.page + '_' + #request.pagination.size + '_' +"
              + " #request.pagination.sortBy + '_' + #request.pagination.sortDirection")
  @CircuitBreaker(name = "database")
  @Retry(name = "database")
  public Page<TrafficResponseDto> queryTrafficBySrcIp(
      String srcIp, TrafficQueryRequestDto request) {
    request.setSrcIp(srcIp);
    return queryTraffic(request);
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

  private TrafficResponseDto toResponse(TrafficWindowProjection aggregate) {
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
        .ingestedAt(aggregate.getIngestedAt())
        .build();
  }

  private Pageable createPageable(TrafficQueryRequestDto request) {
    String sortBy = request.getPagination().getSortBy();
    if (!ALLOWED_SORT_FIELDS.contains(sortBy)) {
      throw new IllegalArgumentException("Unsupported traffic sort field: " + sortBy);
    }
    return PageRequest.of(
        request.getPagination().getPage(),
        request.getPagination().getSize(),
        Sort.by(Sort.Direction.fromString(request.getPagination().getSortDirection()), sortBy));
  }
}
