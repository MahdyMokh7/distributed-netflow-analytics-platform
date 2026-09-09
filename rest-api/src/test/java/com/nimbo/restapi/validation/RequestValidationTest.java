package com.nimbo.restapi.validation;

import com.nimbo.restapi.dto.request.BandwidthQueryRequestDto;
import com.nimbo.restapi.dto.request.PaginationRequestDto;
import com.nimbo.restapi.dto.request.TimeRangeRequestDto;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class RequestValidationTest {

    private static ValidatorFactory validatorFactory;
    private static Validator validator;

    @BeforeAll
    static void createValidator() {
        validatorFactory = Validation.buildDefaultValidatorFactory();
        validator = validatorFactory.getValidator();
    }

    @AfterAll
    static void closeValidator() {
        validatorFactory.close();
    }

    @Test
    void givenValidPastRangeWhenValidatedThenNoViolationsAreReported() {
        // Given: a valid seven-day historical range
        TimeRangeRequestDto request = TimeRangeRequestDto.builder()
                .startTime(Instant.parse("2026-01-01T00:00:00Z"))
                .endTime(Instant.parse("2026-01-08T00:00:00Z"))
                .build();

        // When: the request is validated
        var violations = validator.validate(request);

        // Then: the exact maximum range is accepted
        assertThat(violations).isEmpty();
    }

    @Test
    void givenEqualRangeBoundsWhenValidatedThenEndTimeIsRejected() {
        // Given: a range with identical boundaries
        TimeRangeRequestDto request = TimeRangeRequestDto.builder()
                .startTime(Instant.parse("2026-01-01T00:00:00Z"))
                .endTime(Instant.parse("2026-01-01T00:00:00Z"))
                .build();

        // When: the request is validated
        var violations = validator.validate(request);

        // Then: the invalid end boundary is identified
        assertThat(violations).singleElement().satisfies(violation -> {
            assertThat(violation.getPropertyPath()).hasToString("endTime");
            assertThat(violation.getMessage()).isEqualTo("End time must be after start time");
        });
    }

    @Test
    void givenExcessiveRangeWhenValidatedThenStartTimeIsRejected() {
        // Given: a historical range longer than seven days
        TimeRangeRequestDto request = TimeRangeRequestDto.builder()
                .startTime(Instant.parse("2025-12-31T23:59:59Z"))
                .endTime(Instant.parse("2026-01-08T00:00:00Z"))
                .build();

        // When: the request is validated
        var violations = validator.validate(request);

        // Then: the excessive duration is reported
        assertThat(violations).singleElement().satisfies(violation -> {
            assertThat(violation.getPropertyPath()).hasToString("startTime");
            assertThat(violation.getMessage()).isEqualTo("Time range cannot exceed 7 days");
        });
    }

    @Test
    void givenInvalidPaginationAndLimitWhenValidatedThenAllBoundsAreReported() {
        // Given: pagination and bandwidth values outside allowed bounds
        PaginationRequestDto pagination = PaginationRequestDto.builder().page(-1).size(101).build();
        BandwidthQueryRequestDto bandwidth = BandwidthQueryRequestDto.builder()
                .startTime(Instant.parse("2026-01-01T00:00:00Z"))
                .endTime(Instant.parse("2026-01-02T00:00:00Z"))
                .limit(101)
                .build();

        // When: both requests are validated
        var paginationViolations = validator.validate(pagination);
        var bandwidthViolations = validator.validate(bandwidth);

        // Then: page size and limit constraints all fail
        assertThat(paginationViolations)
                .extracting(violation -> violation.getPropertyPath().toString())
                .containsExactlyInAnyOrder("page", "size");
        assertThat(bandwidthViolations)
                .extracting(violation -> violation.getPropertyPath().toString())
                .containsExactly("limit");
    }

    @Test
    void givenIpLiteralsWhenValidatedThenOnlyMalformedValueFails() {
        // Given: valid IPv4 IPv6 and malformed addresses
        IpAddressValidator ipValidator = new IpAddressValidator();

        // When: each literal is checked
        boolean ipv4 = ipValidator.isValid("10.10.61.52", null);
        boolean ipv6 = ipValidator.isValid("2001:db8::1", null);
        boolean malformed = ipValidator.isValid("999.10.61.52", null);

        // Then: only real address literals are accepted
        assertThat(ipv4).isTrue();
        assertThat(ipv6).isTrue();
        assertThat(malformed).isFalse();
    }
}
