package com.nimbo.restapi.dto.request;

import com.nimbo.restapi.validation.ValidTimeRange;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.time.Instant;

@Data
@SuperBuilder
@NoArgsConstructor
@ValidTimeRange
public class TimeRangeRequestDto {

    @NotNull
    private Instant startTime;

    @NotNull
    private Instant endTime;
}
