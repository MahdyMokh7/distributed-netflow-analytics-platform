package com.nimbo.analyticsquery.client.restapi;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbo.analyticsquery.client.restapi.dto.BandwidthRow;
import com.nimbo.analyticsquery.client.restapi.dto.ConnectionRow;
import com.nimbo.analyticsquery.client.restapi.dto.ProtocolRow;
import com.nimbo.analyticsquery.client.restapi.dto.TrafficRow;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

@Component
public class RestApiClient {

  private static final ParameterizedTypeReference<RestApiResponse<List<TrafficRow>>>
      TRAFFIC_RESPONSE = new ParameterizedTypeReference<>() {};

  private static final ParameterizedTypeReference<RestApiResponse<List<ConnectionRow>>>
      CONNECTION_RESPONSE = new ParameterizedTypeReference<>() {};

  private static final ParameterizedTypeReference<RestApiResponse<List<BandwidthRow>>>
      BANDWIDTH_RESPONSE = new ParameterizedTypeReference<>() {};

  private static final ParameterizedTypeReference<RestApiResponse<List<ProtocolRow>>>
      PROTOCOL_RESPONSE = new ParameterizedTypeReference<>() {};

  private final RestClient restClient;
  private final ObjectMapper objectMapper;

  public RestApiClient(
      @Qualifier("restApiRestClient") RestClient restClient, ObjectMapper objectMapper) {

    this.restClient = restClient;
    this.objectMapper = objectMapper;
  }

  public RestApiResponse<List<TrafficRow>> getTrafficWindows(
      String sourceIp,
      Instant startTime,
      Instant endTime,
      RestApiPageRequest<TrafficSortField> pageRequest) {

    return execute(
        restClient
            .get()
            .uri(
                uriBuilder -> {
                  uriBuilder
                      .pathSegment("traffic")
                      .queryParam("startTime", startTime)
                      .queryParam("endTime", endTime)
                      .queryParam("page", pageRequest.page())
                      .queryParam("size", pageRequest.size())
                      .queryParam("sortBy", pageRequest.sortBy().parameterValue())
                      .queryParam("sortDirection", pageRequest.sortDirection());

                  if (sourceIp != null) {
                    uriBuilder.queryParam("srcIp", sourceIp);
                  }

                  return uriBuilder.build();
                }),
        TRAFFIC_RESPONSE);
  }

  public RestApiResponse<List<ConnectionRow>> getConnectionWindows(
      String sourceIp,
      String destinationIp,
      Instant startTime,
      Instant endTime,
      RestApiPageRequest<ConnectionSortField> pageRequest) {

    return execute(
        restClient
            .get()
            .uri(
                uriBuilder -> {
                  uriBuilder
                      .pathSegment("connections")
                      .queryParam("startTime", startTime)
                      .queryParam("endTime", endTime)
                      .queryParam("page", pageRequest.page())
                      .queryParam("size", pageRequest.size())
                      .queryParam("sortBy", pageRequest.sortBy().parameterValue())
                      .queryParam("sortDirection", pageRequest.sortDirection());

                  if (sourceIp != null) {
                    uriBuilder.queryParam("srcIp", sourceIp);
                  }

                  if (destinationIp != null) {
                    uriBuilder.queryParam("dstIp", destinationIp);
                  }

                  return uriBuilder.build();
                }),
        CONNECTION_RESPONSE);
  }

  public RestApiResponse<List<ConnectionRow>> getConnectionWindowsForIp(
      String ip,
      Instant startTime,
      Instant endTime,
      RestApiPageRequest<ConnectionSortField> pageRequest) {

    return execute(
        restClient
            .get()
            .uri(
                uriBuilder ->
                    uriBuilder
                        .pathSegment("connections", ip)
                        .queryParam("startTime", startTime)
                        .queryParam("endTime", endTime)
                        .queryParam("page", pageRequest.page())
                        .queryParam("size", pageRequest.size())
                        .queryParam("sortBy", pageRequest.sortBy().parameterValue())
                        .queryParam("sortDirection", pageRequest.sortDirection())
                        .build()),
        CONNECTION_RESPONSE);
  }

  public RestApiResponse<List<BandwidthRow>> getTopBandwidthIps(
      Instant startTime, Instant endTime, int limit) {

    if (limit < 1 || limit > 100) {
      throw new IllegalArgumentException("limit must be between 1 and 100");
    }

    return execute(
        restClient
            .get()
            .uri(
                uriBuilder ->
                    uriBuilder
                        .pathSegment("bandwidth", "top")
                        .queryParam("startTime", startTime)
                        .queryParam("endTime", endTime)
                        .queryParam("limit", limit)
                        .build()),
        BANDWIDTH_RESPONSE);
  }

  public RestApiResponse<List<ProtocolRow>> getProtocolUsageWindows(
      Instant startTime, Instant endTime) {

    return execute(
        restClient
            .get()
            .uri(
                uriBuilder ->
                    uriBuilder
                        .pathSegment("protocols")
                        .queryParam("startTime", startTime)
                        .queryParam("endTime", endTime)
                        .build()),
        PROTOCOL_RESPONSE);
  }

  private <T> RestApiResponse<T> execute(
      RestClient.RequestHeadersSpec<?> request,
      ParameterizedTypeReference<RestApiResponse<T>> responseType) {

    try {
      RestApiResponse<T> response =
          request
              .retrieve()
              .onStatus(
                  HttpStatusCode::isError,
                  (httpRequest, httpResponse) -> {
                    throw mapHttpError(httpResponse);
                  })
              .body(responseType);

      return validateSuccessfulResponse(response);

    } catch (RestApiClientException exception) {
      throw exception;

    } catch (ResourceAccessException exception) {
      throw new RestApiClientException(
          RestApiClientException.Kind.UNAVAILABLE,
          null,
          "rest-api request timed out or the service is unreachable",
          exception);

    } catch (RestClientException exception) {
      throw new RestApiClientException(
          RestApiClientException.Kind.INVALID_RESPONSE,
          null,
          "rest-api response could not be processed",
          exception);
    }
  }

  private <T> RestApiResponse<T> validateSuccessfulResponse(RestApiResponse<T> response) {

    if (response == null) {
      throw invalidResponse("rest-api returned an empty HTTP response");
    }

    if (!Boolean.TRUE.equals(response.success())) {
      throw invalidResponse("rest-api returned an unsuccessful 2xx response");
    }

    if (response.data() == null) {
      throw invalidResponse("rest-api successful response contains no data");
    }

    return response;
  }

  private RestApiClientException mapHttpError(ClientHttpResponse response) throws IOException {

    int status = response.getStatusCode().value();
    RestApiErrorBody body = readErrorBody(response);
    RestApiClientException.Kind kind =
        classifyHttpError(status, body == null ? null : body.success());

    return new RestApiClientException(kind, status, errorMessage(body, status));
  }

  private RestApiErrorBody readErrorBody(ClientHttpResponse response) {

    try {
      return objectMapper.readValue(response.getBody(), RestApiErrorBody.class);

    } catch (IOException exception) {
      return null;
    }
  }

  static RestApiClientException.Kind classifyHttpError(int status, Boolean responseSuccess) {
    if (status == 404 && Boolean.FALSE.equals(responseSuccess)) {
      return RestApiClientException.Kind.NO_DATA;
    }

    if (status == 400) {
      return RestApiClientException.Kind.BAD_REQUEST;
    }

    if (status == 408 || status == 429 || status >= 500) {
      return RestApiClientException.Kind.UNAVAILABLE;
    }

    return RestApiClientException.Kind.INVALID_RESPONSE;
  }

  private String errorMessage(RestApiErrorBody body, int status) {

    if (body != null && body.message() != null && !body.message().isBlank()) {
      return body.message();
    }

    return "rest-api request failed with HTTP status " + status;
  }

  private RestApiClientException invalidResponse(String message) {

    return new RestApiClientException(RestApiClientException.Kind.INVALID_RESPONSE, null, message);
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  private record RestApiErrorBody(Boolean success, String message) {}
}
