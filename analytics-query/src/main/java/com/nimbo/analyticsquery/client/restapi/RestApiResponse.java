package com.nimbo.analyticsquery.client.restapi;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.Instant;

@JsonIgnoreProperties(ignoreUnknown = true)
public record RestApiResponse<T>(
    Boolean success, String message, T data, RestApiPagination pagination, Instant timestamp) {}
