package com.nimbo.restapi.dto.request;

import com.nimbo.restapi.validation.ValidIpAddress;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
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
public class BandwidthQueryRequestDto extends TimeRangeRequestDto {

  @ValidIpAddress private String ip;

  @Min(value = 1, message = "Limit must be at least 1")
  @Max(value = 100, message = "Limit must not exceed 100")
  private Integer limit;
}
