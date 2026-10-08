package com.nimbo.analyticsquery.client.restapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbo.analyticsquery.client.restapi.dto.BandwidthRow;
import com.nimbo.analyticsquery.client.restapi.dto.ConnectionRow;
import com.nimbo.analyticsquery.config.RestApiClientConfiguration;
import com.nimbo.analyticsquery.config.RestApiProperties;
import com.nimbo.analyticsquery.support.RestApiTestServerExtension;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.web.client.RestClient;

class RestApiClientTest {

  private static final Instant START = Instant.parse("2026-10-01T08:00:00Z");
  private static final Instant END = Instant.parse("2026-10-01T09:00:00Z");

  @RegisterExtension
  static final RestApiTestServerExtension REST_API = new RestApiTestServerExtension();

  @Test
  void givenConnectionRequestWhenExecutedThenParametersAndResponseArePreserved() {
    // Given: a successful REST API connection response
    REST_API.respondWith(200, successfulConnectionResponse());
    RestApiClient client = client(Duration.ofSeconds(1));
    RestApiPageRequest<ConnectionSortField> page =
        new RestApiPageRequest<>(
            1,
            25,
            ConnectionSortField.OUT_BYTES,
            RestApiPageRequest.SortDirection.ASC);

    // When: the centralized client requests filtered connection windows
    RestApiResponse<java.util.List<ConnectionRow>> response =
        client.getConnectionWindows("10.0.0.1", "10.0.0.2", START, END, page);

    // Then: the endpoint contract and typed response are preserved
    assertThat(response.data()).hasSize(1);
    assertThat(REST_API.lastRequestUri().getPath()).isEqualTo("/api/connections");
    assertThat(REST_API.lastRequestUri().getRawQuery())
        .contains(
            "startTime=2026-10-01T08:00:00Z",
            "endTime=2026-10-01T09:00:00Z",
            "page=1",
            "size=25",
            "sortBy=outBytes",
            "sortDirection=ASC",
            "srcIp=10.0.0.1",
            "dstIp=10.0.0.2");
  }

  @Test
  void givenTopBandwidthResponseWhenExecutedThenSummaryContractIsPreserved() {
    REST_API.respondWith(
        200,
        """
        {
          "success": true,
          "data": [{
            "ip": "83.222.96.124",
            "inboundBytes": 9751430,
            "outboundBytes": 8401767,
            "totalBytes": 18153197,
            "inboundPackets": 7892,
            "outboundPackets": 6413,
            "totalPackets": 14305
          }],
          "timestamp": "2026-10-08T11:30:00Z"
        }
        """);
    RestApiClient client = client(Duration.ofSeconds(1));

    RestApiResponse<java.util.List<BandwidthRow>> response =
        client.getTopBandwidthIps(START, END, 10);

    assertThat(response.data())
        .singleElement()
        .satisfies(
            row -> {
              assertThat(row.ip()).isEqualTo("83.222.96.124");
              assertThat(row.totalBytes()).isEqualTo(java.math.BigInteger.valueOf(18_153_197));
              assertThat(row.totalPackets()).isEqualTo(java.math.BigInteger.valueOf(14_305));
            });
  }

  @Test
  void givenMalformedSuccessResponseWhenExecutedThenInvalidResponseIsReported() {
    // Given: a downstream response that is not valid JSON
    REST_API.respondWith(200, "not-json");
    RestApiClient client = client(Duration.ofSeconds(1));

    // When: the malformed response is decoded
    var failure =
        assertThatThrownBy(() -> client.getProtocolUsageWindows(START, END));

    // Then: the protocol failure is controlled
    failure
        .isInstanceOfSatisfying(
            RestApiClientException.class,
            exception ->
                assertThat(exception.kind())
                    .isEqualTo(RestApiClientException.Kind.INVALID_RESPONSE));
  }

  @Test
  void givenSlowDownstreamWhenExecutedThenUnavailableIsReported() {
    // Given: a dependency that does not answer within the read timeout
    REST_API.respondWith(200, successfulConnectionResponse());
    REST_API.holdResponse();
    RestApiClient client = client(Duration.ofMillis(100));

    // When: the client reaches its configured timeout
    var failure =
        assertThatThrownBy(
            () ->
                client.getConnectionWindowsForIp(
                    "10.0.0.1",
                    START,
                    END,
                    new RestApiPageRequest<>(
                        0,
                        10,
                        ConnectionSortField.WINDOW_START,
                        RestApiPageRequest.SortDirection.DESC)));

    // Then: timeout is exposed as a controlled dependency failure
    failure
        .isInstanceOfSatisfying(
            RestApiClientException.class,
            exception ->
                assertThat(exception.kind()).isEqualTo(RestApiClientException.Kind.UNAVAILABLE));
  }

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

  private RestApiClient client(Duration readTimeout) {
    RestApiProperties properties =
        new RestApiProperties(
            REST_API.baseUrl(), new RestApiProperties.Timeout(Duration.ofSeconds(1), readTimeout));
    RestClient restClient =
        new RestApiClientConfiguration()
            .restApiRestClient(RestClient.builder(), properties);
    ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
    return new RestApiClient(restClient, objectMapper);
  }

  private String successfulConnectionResponse() {
    return """
        {
          "success": true,
          "data": [{
            "id": "connection-id",
            "srcIp": "10.0.0.1",
            "dstIp": "10.0.0.2",
            "windowStart": "2026-10-01T08:00:00Z",
            "windowEnd": "2026-10-01T09:00:00Z",
            "inBytes": 10,
            "outBytes": 20,
            "totalBytes": 30,
            "inPackets": 1,
            "outPackets": 2,
            "totalPackets": 3,
            "ingestedAt": "2026-10-01T09:00:00Z"
          }],
          "timestamp": "2026-10-01T09:00:00Z"
        }
        """;
  }
}
