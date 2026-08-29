package com.nimbo.restapi.dto.request;

import com.nimbo.restapi.validation.ValidIpAddress;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class ConnectionQueryRequestDto extends TimeRangeRequestDto {

    @ValidIpAddress
    private String srcIp;

    @ValidIpAddress
    private String dstIp;

    private Integer srcPort;

    private Integer dstPort;

    private Integer l4Protocol;

    private PaginationRequestDto pagination;
}