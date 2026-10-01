package com.nimbo.analyticsquery.tool;

import com.nimbo.analyticsquery.client.restapi.RestApiClientException;
import com.nimbo.analyticsquery.model.AnalysisStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ToolExecutionExceptionTest {

    @Test
    void givenUnavailableClientFailureWhenTranslatedThenDependencyStatusIsUsed() {
        // Given: an unavailable REST API client failure
        RestApiClientException clientFailure = failure(RestApiClientException.Kind.UNAVAILABLE);

        // When: the tool boundary translates the client failure
        ToolExecutionException result = ToolExecutionException.from(clientFailure);

        // Then: dependency unavailability is reported without exposing the client message
        assertThat(result.status()).isEqualTo(AnalysisStatus.DEPENDENCY_UNAVAILABLE);
        assertThat(result.userMessage()).isEqualTo("Analytics data service is temporarily unavailable");
        assertThat(result.getCause()).isSameAs(clientFailure);
    }

    @ParameterizedTest
    @EnumSource(
            value = RestApiClientException.Kind.class,
            names = {"BAD_REQUEST", "INVALID_RESPONSE"})
    void givenNonAvailabilityClientFailureWhenTranslatedThenExecutionFailureIsUsed(
            RestApiClientException.Kind kind) {

        // Given: a controlled non-availability REST API client failure
        RestApiClientException clientFailure = failure(kind);

        // When: the tool boundary translates the client failure
        ToolExecutionException result = ToolExecutionException.from(clientFailure);

        // Then: execution failure is reported without exposing the client message
        assertThat(result.status()).isEqualTo(AnalysisStatus.EXECUTION_FAILED);
        assertThat(result.userMessage()).isEqualTo("Analytics tool execution failed");
        assertThat(result.getCause()).isSameAs(clientFailure);
    }

    @Test
    void givenNoDataClientResultWhenTranslatedThenIncorrectTranslationIsRejected() {
        // Given: a no-data REST API result that tools must handle as success
        RestApiClientException clientFailure = failure(RestApiClientException.Kind.NO_DATA);

        // When: failure translation is incorrectly attempted
        var assertion = assertThatThrownBy(() -> ToolExecutionException.from(clientFailure));

        // Then: the invalid failure path is rejected
        assertion
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("NO_DATA must be handled as an empty analytics result")
                .hasCause(clientFailure);
    }

    private static RestApiClientException failure(RestApiClientException.Kind kind) {
        return new RestApiClientException(kind, null, "sensitive downstream detail");
    }
}
