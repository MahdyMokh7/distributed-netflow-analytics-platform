package com.nimbo.restapi.service;

import com.nimbo.restapi.dto.request.ProtocolQueryRequestDto;
import com.nimbo.restapi.dto.response.ProtocolResponseDto;
import java.util.List;

public interface ProtocolQueryService {

  List<ProtocolResponseDto> queryProtocolUsage(ProtocolQueryRequestDto request);

  ProtocolResponseDto queryProtocolUsageByProtocol(
      Double l7Protocol, ProtocolQueryRequestDto request);
}
