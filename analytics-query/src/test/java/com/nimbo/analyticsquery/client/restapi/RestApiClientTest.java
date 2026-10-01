package com.nimbo.analyticsquery.client.restapi;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RestApiClientTest {

    @Test
    void givenRestApiNoDataEnvelopeWhenClassifiedThenNoDataIsReturned() {
        // Given: a REST API 404 error envelope
        int status = 404;

        // When: the HTTP error is classified
        RestApiClientException.Kind kind = RestApiClient.classifyHttpError(status, false);

        // Then: the response is recognized as an empty data result
        assertThat(kind).isEqualTo(RestApiClientException.Kind.NO_DATA);
    }

    @Test
    void givenUnknownNotFoundResponseWhenClassifiedThenInvalidResponseIsReturned() {
        // Given: a 404 without the REST API error envelope
        int status = 404;

        // When: the HTTP error is classified
        RestApiClientException.Kind kind = RestApiClient.classifyHttpError(status, null);

        // Then: the response is not mistaken for an empty data result
        assertThat(kind).isEqualTo(RestApiClientException.Kind.INVALID_RESPONSE);
    }

    @Test
    void givenUnavailableResponseWhenClassifiedThenUnavailableIsReturned() {
        // Given: a REST API service-unavailable response
        int status = 503;

        // When: the HTTP error is classified
        RestApiClientException.Kind kind = RestApiClient.classifyHttpError(status, false);

        // Then: the dependency failure is classified as unavailable
        assertThat(kind).isEqualTo(RestApiClientException.Kind.UNAVAILABLE);
    }
}
