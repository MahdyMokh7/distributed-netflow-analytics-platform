package com.nimbo.restapi.dto.request;

import com.nimbo.restapi.validation.ValidIpAddress;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import lombok.Builder;
@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class BandwidthQueryRequestDto extends TimeRangeRequestDto {

    @ValidIpAddress
    private String ip;

    @Builder.Default
    private Integer limit = 10;
}