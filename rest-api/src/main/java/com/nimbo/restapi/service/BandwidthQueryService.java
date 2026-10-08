package com.nimbo.restapi.service;

import com.nimbo.restapi.dto.request.BandwidthQueryRequestDto;
import com.nimbo.restapi.dto.response.BandwidthResponseDto;
import com.nimbo.restapi.dto.response.TopBandwidthResponseDto;
import java.util.List;

public interface BandwidthQueryService {

  List<TopBandwidthResponseDto> getTopBandwidthIps(BandwidthQueryRequestDto request);

  BandwidthResponseDto queryBandwidthByIp(String ip, BandwidthQueryRequestDto request);
}
