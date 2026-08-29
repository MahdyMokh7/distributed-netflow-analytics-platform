package com.nimbo.restapi.service;

import com.nimbo.restapi.dto.response.realtime.RealtimeBandwidthResponseDto;
import com.nimbo.restapi.dto.response.realtime.RealtimeConnectionResponseDto;
import com.nimbo.restapi.dto.response.realtime.RealtimeProtocolResponseDto;
import com.nimbo.restapi.dto.response.realtime.RealtimeTrafficResponseDto;
import org.springframework.data.domain.Page;

import java.time.Instant;
import java.util.List;

public interface RealtimeQueryService {

    RealtimeTrafficResponseDto getTrafficByIp(
            String ip,
            Instant startTime,
            Instant endTime
    );

    Page<RealtimeConnectionResponseDto> getConnectionsByIp(
            String ip,
            Instant startTime,
            Instant endTime,
            int page,
            int size
    );

    List<RealtimeBandwidthResponseDto> getTopBandwidthIps(
            Instant startTime,
            Instant endTime,
            int limit
    );

    List<RealtimeProtocolResponseDto> getProtocolUsage(
            Instant startTime,
            Instant endTime
    );
}