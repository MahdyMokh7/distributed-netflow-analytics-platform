package com.nimbo.analyticsquery.tool;

import com.nimbo.analyticsquery.client.restapi.RestApiClientException;
import com.nimbo.analyticsquery.model.AnalysisStatus;

public final class ToolExecutionException extends RuntimeException {

    private final AnalysisStatus status;
    private final String userMessage;

    private ToolExecutionException(
            AnalysisStatus status,
            String userMessage,
            Throwable cause) {

        super(userMessage, cause);
        this.status = status;
        this.userMessage = userMessage;
    }

    public static ToolExecutionException from(RestApiClientException exception) {
        return switch (exception.kind()) {
            case UNAVAILABLE -> new ToolExecutionException(
                    AnalysisStatus.DEPENDENCY_UNAVAILABLE,
                    "Analytics data service is temporarily unavailable",
                    exception);

            case BAD_REQUEST, INVALID_RESPONSE -> new ToolExecutionException(
                    AnalysisStatus.EXECUTION_FAILED,
                    "Analytics tool execution failed",
                    exception);

            case NO_DATA -> throw new IllegalArgumentException(
                    "NO_DATA must be handled as an empty analytics result",
                    exception);
        };
    }

    public AnalysisStatus status() {
        return status;
    }

    public String userMessage() {
        return userMessage;
    }
}
