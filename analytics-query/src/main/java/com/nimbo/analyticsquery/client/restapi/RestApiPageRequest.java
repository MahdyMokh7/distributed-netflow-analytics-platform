package com.nimbo.analyticsquery.client.restapi;

import java.util.Objects;

public record RestApiPageRequest<S extends Enum<S>>(
        int page,
        int size,
        S sortBy,
        SortDirection sortDirection) {

    public RestApiPageRequest {
        if (page < 0) {
            throw new IllegalArgumentException("page must be greater than or equal to 0");
        }

        if (size < 1 || size > 100) {
            throw new IllegalArgumentException("size must be between 1 and 100");
        }

        Objects.requireNonNull(sortBy, "sortBy must not be null");
        Objects.requireNonNull(sortDirection, "sortDirection must not be null");
    }

    public enum SortDirection {
        ASC,
        DESC
    }
}
