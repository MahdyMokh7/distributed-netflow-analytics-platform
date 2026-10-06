package com.nimbo.restapi.service;

import com.nimbo.restapi.dto.request.ConnectionQueryRequestDto;
import com.nimbo.restapi.dto.response.ConnectionResponseDto;
import org.springframework.data.domain.Page;

public interface ConnectionQueryService {

  Page<ConnectionResponseDto> queryConnections(ConnectionQueryRequestDto request);

  Page<ConnectionResponseDto> queryConnectionsByIp(String ip, ConnectionQueryRequestDto request);
}
