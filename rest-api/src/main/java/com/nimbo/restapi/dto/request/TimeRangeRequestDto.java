package com.nimbo.restapi.dto.request;

import jakarta.validation.constraints.NotNull;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.time.Instant;

@Data
@SuperBuilder
@NoArgsConstructor
public class TimeRangeRequestDto {

    @NotNull
    private Instant startTime;

    @NotNull
    private Instant endTime;
}