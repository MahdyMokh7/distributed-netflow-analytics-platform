package com.nimbo.restapi.service.impl;

import com.nimbo.restapi.dto.response.realtime.RealtimeBandwidthResponseDto;
import com.nimbo.restapi.dto.response.realtime.RealtimeConnectionResponseDto;
import com.nimbo.restapi.dto.response.realtime.RealtimeProtocolResponseDto;
import com.nimbo.restapi.dto.response.realtime.RealtimeTrafficResponseDto;
import com.nimbo.restapi.repository.realtime.RealtimeQueryRepository;
import com.nimbo.restapi.service.RealtimeQueryService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

@Service
@RequiredArgsConstructor
public class RealtimeQueryServiceImpl
        implements RealtimeQueryService {

    private static final int MAX_PAGE_SIZE = 100;
    private static final int MAX_TOP_LIMIT = 100;

    private final RealtimeQueryRepository repository;

    @Override
    public RealtimeTrafficResponseDto getTrafficByIp(
            String ip,
            Instant startTime,
            Instant endTime
    ) {
        return repository.findTrafficByIp(
                ip,
                startTime,
                endTime
        );
    }

    @Override
    public Page<RealtimeConnectionResponseDto> getConnectionsByIp(
            String ip,
            Instant startTime,
            Instant endTime,
            int page,
            int size
    ) {
        int effectivePage =
                Math.max(page, 0);

        int effectiveSize =
                Math.min(
                        Math.max(size, 1),
                        MAX_PAGE_SIZE
                );

        return repository.findConnectionsByIp(
                ip,
                startTime,
                endTime,
                effectivePage,
                effectiveSize
        );
    }

    @Override
    public List<RealtimeBandwidthResponseDto> getTopBandwidthIps(
            Instant startTime,
            Instant endTime,
            int limit
    ) {
        int effectiveLimit =
                Math.min(
                        Math.max(limit, 1),
                        MAX_TOP_LIMIT
                );

        return repository.findTopBandwidthIps(
                startTime,
                endTime,
                effectiveLimit
        );
    }

    @Override
    public List<RealtimeProtocolResponseDto> getProtocolUsage(
            Instant startTime,
            Instant endTime
    ) {
        return repository.findProtocolUsage(
                startTime,
                endTime
        );
    }
}