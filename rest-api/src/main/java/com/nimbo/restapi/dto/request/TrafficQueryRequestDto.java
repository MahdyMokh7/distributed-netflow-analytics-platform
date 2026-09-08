package com.nimbo.restapi.dto.request;

import com.nimbo.restapi.validation.ValidIpAddress;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class TrafficQueryRequestDto extends TimeRangeRequestDto {

    @ValidIpAddress
    private String srcIp;

    private PaginationRequestDto pagination;
}
