package com.nimbo.restapi.service;

import com.nimbo.restapi.dto.response.Starrocks.StarrocksBandwidthResponseDto;
import com.nimbo.restapi.dto.response.Starrocks.StarrocksConnectionResponseDto;
import com.nimbo.restapi.dto.response.Starrocks.StarrocksProtocolResponseDto;
import com.nimbo.restapi.dto.response.Starrocks.StarrocksTrafficResponseDto;
import java.time.Instant;
import java.util.List;
import org.springframework.data.domain.Page;

public interface StarRocksQueryService {

  List<StarrocksTrafficResponseDto> getTrafficByIp(String ip, Instant startTime, Instant endTime);

  Page<StarrocksConnectionResponseDto> getConnectionsByIp(
      String ip, Instant startTime, Instant endTime, int page, int size);

  List<StarrocksBandwidthResponseDto> getTopBandwidthIps(
      Instant startTime, Instant endTime, int limit);

  List<StarrocksProtocolResponseDto> getProtocolUsage(Instant startTime, Instant endTime);
}
