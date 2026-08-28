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
public class BandwidthQueryRequestDto extends TimeRangeRequestDto {

    @ValidIpAddress
    private String ip;

    @Builder.Default
    private Integer limit = 10;
}