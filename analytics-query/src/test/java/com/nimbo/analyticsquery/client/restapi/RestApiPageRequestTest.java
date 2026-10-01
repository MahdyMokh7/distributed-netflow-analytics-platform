package com.nimbo.analyticsquery.client.restapi;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RestApiPageRequestTest {

    @Test
    void givenTrafficSortFieldWhenRequestIsCreatedThenTypedSortIsPreserved() {
        // Given: valid traffic pagination values
        TrafficSortField sortField = TrafficSortField.IN_BYTES;

        // When: a traffic page request is created
        RestApiPageRequest<TrafficSortField> request = new RestApiPageRequest<>(
                0, 20, sortField, RestApiPageRequest.SortDirection.DESC);

        // Then: the REST sort parameter is the supported traffic field
        assertThat(request.sortBy().parameterValue()).isEqualTo("inBytes");
    }

    @Test
    void givenConnectionSortFieldWhenRequestIsCreatedThenTypedSortIsPreserved() {
        // Given: valid connection pagination values
        ConnectionSortField sortField = ConnectionSortField.DESTINATION_IP;

        // When: a connection page request is created
        RestApiPageRequest<ConnectionSortField> request = new RestApiPageRequest<>(
                1, 50, sortField, RestApiPageRequest.SortDirection.ASC);

        // Then: the REST sort parameter is the supported connection field
        assertThat(request.sortBy().parameterValue()).isEqualTo("dstIp");
    }

    @Test
    void givenMissingSortFieldWhenRequestIsCreatedThenItIsRejected() {
        // Given: no traffic sort field
        TrafficSortField sortField = null;

        // When: a page request is created
        var assertion = assertThatThrownBy(() -> new RestApiPageRequest<>(
                0, 20, sortField, RestApiPageRequest.SortDirection.DESC));

        // Then: the missing sort field is rejected
        assertion.isInstanceOf(NullPointerException.class)
                .hasMessage("sortBy must not be null");
    }
}
