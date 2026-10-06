package com.nimbo.restapi.service;

import com.nimbo.restapi.dto.request.TrafficQueryRequestDto;
import com.nimbo.restapi.dto.response.TrafficResponseDto;
import org.springframework.data.domain.Page;

public interface TrafficQueryService {

  Page<TrafficResponseDto> queryTraffic(TrafficQueryRequestDto request);

  Page<TrafficResponseDto> queryTrafficBySrcIp(String srcIp, TrafficQueryRequestDto request);
}
