package com.nimbo.restapi.dto.request;

import com.nimbo.restapi.validation.ValidTimeRange;
import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
@NoArgsConstructor
@ValidTimeRange
public class TimeRangeRequestDto {

  @NotNull private Instant startTime;

  @NotNull private Instant endTime;
}
