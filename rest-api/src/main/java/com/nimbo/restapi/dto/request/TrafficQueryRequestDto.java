package com.nimbo.restapi.dto.request;

import com.nimbo.restapi.dto.validation.ValidIpAddress;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class TrafficQueryRequestDto extends TimeRangeRequestDto {

    @ValidIpAddress
    private String srcIp;

    @ValidIpAddress
    private String dstIp;

    @Builder.Default
    private PaginationRequestDto pagination = PaginationRequestDto.builder()
            .page(0)
            .size(20)
            .build();
}