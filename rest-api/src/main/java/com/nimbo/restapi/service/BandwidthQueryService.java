package com.nimbo.restapi.service;

import com.nimbo.restapi.dto.request.BandwidthQueryRequestDto;
import com.nimbo.restapi.dto.response.BandwidthResponseDto;

import java.util.List;

public interface BandwidthQueryService {

    List<BandwidthResponseDto> getTopBandwidthIps(BandwidthQueryRequestDto request);

    BandwidthResponseDto queryBandwidthByIp(String ip, BandwidthQueryRequestDto request);
}