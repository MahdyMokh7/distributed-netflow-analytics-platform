package com.nimbo.restapi.validation;

import com.nimbo.restapi.dto.request.TimeRangeRequestDto;
import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;

import java.time.Duration;
import java.time.Instant;

@Slf4j
public class TimeRangeValidator implements ConstraintValidator<ValidTimeRange, TimeRangeRequestDto> {

    @Value("${api.time-range.max-days:7}")
    private int maxDays;

    @Override
    public boolean isValid(TimeRangeRequestDto request, ConstraintValidatorContext context) {
        if (request == null) {
            return true;
        }

        Instant start = request.getStartTime();
        Instant end = request.getEndTime();

        if (start == null || end == null) {
            return true;
        }

        if (end.isBefore(start)) {
            context.disableDefaultConstraintViolation();
            context.buildConstraintViolationWithTemplate("End time must be after start time")
                    .addPropertyNode("endTime")
                    .addConstraintViolation();
            return false;
        }

        Instant now = Instant.now();
        if (end.isAfter(now)) {
            context.disableDefaultConstraintViolation();
            context.buildConstraintViolationWithTemplate("Cannot query future dates")
                    .addPropertyNode("endTime")
                    .addConstraintViolation();
            return false;
        }

        Duration duration = Duration.between(start, end);
        if (duration.toDays() > maxDays) {
            context.disableDefaultConstraintViolation();
            context.buildConstraintViolationWithTemplate(
                            "Time range cannot exceed " + maxDays + " days")
                    .addPropertyNode("startTime")
                    .addConstraintViolation();
            return false;
        }

        return true;
    }
}