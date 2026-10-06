package com.nimbo.analyticsquery.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.net.URI;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "rest-api")
public record RestApiProperties(@NotNull URI baseUrl, @Valid @NotNull Timeout timeout) {

  public RestApiProperties {
    if (baseUrl != null) {
      String scheme = baseUrl.getScheme();

      if (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme)) {
        throw new IllegalArgumentException("rest-api.base-url must use HTTP or HTTPS");
      }
    }
  }

  public record Timeout(@NotNull Duration connect, @NotNull Duration read) {

    public Timeout {
      requirePositive(connect, "rest-api.timeout.connect");
      requirePositive(read, "rest-api.timeout.read");
    }

    private static void requirePositive(Duration duration, String property) {

      if (duration != null && (duration.isZero() || duration.isNegative())) {
        throw new IllegalArgumentException(property + " must be positive");
      }
    }
  }
}
