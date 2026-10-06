package com.nimbo.restapi.dto.request;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class ProtocolQueryRequestDto extends TimeRangeRequestDto {

  private Double l7Protocol;
}
