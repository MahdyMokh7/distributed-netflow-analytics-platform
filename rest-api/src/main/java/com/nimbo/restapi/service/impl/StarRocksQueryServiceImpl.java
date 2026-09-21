package com.nimbo.restapi.service.impl;

import com.nimbo.restapi.dto.response.Starrocks.StarrocksBandwidthResponseDto;
import com.nimbo.restapi.dto.response.Starrocks.StarrocksConnectionResponseDto;
import com.nimbo.restapi.dto.response.Starrocks.StarrocksProtocolResponseDto;
import com.nimbo.restapi.dto.response.Starrocks.StarrocksTrafficResponseDto;
import com.nimbo.restapi.repository.Starrocks.StarrocksQueryRepository;
import com.nimbo.restapi.service.StarRocksQueryService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

@Service
@RequiredArgsConstructor
public class StarRocksQueryServiceImpl
        implements StarRocksQueryService {

    private static final int MAX_PAGE_SIZE = 100;
    private static final int MAX_TOP_LIMIT = 100;

    private final StarrocksQueryRepository repository;

    @Override
    public StarrocksTrafficResponseDto getTrafficByIp(
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
    public Page<StarrocksConnectionResponseDto> getConnectionsByIp(
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
    public List<StarrocksBandwidthResponseDto> getTopBandwidthIps(
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
    public List<StarrocksProtocolResponseDto> getProtocolUsage(
            Instant startTime,
            Instant endTime
    ) {
        return repository.findProtocolUsage(
                startTime,
                endTime
        );
    }
}