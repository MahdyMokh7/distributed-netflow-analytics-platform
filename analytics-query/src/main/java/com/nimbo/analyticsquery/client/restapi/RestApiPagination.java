package com.nimbo.analyticsquery.client.restapi;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record RestApiPagination(
    long totalElements,
    int totalPages,
    int page,
    int size,
    boolean first,
    boolean last,
    long numberOfElements) {}
