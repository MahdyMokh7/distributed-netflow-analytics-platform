package com.nimbo.starrockswriter.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Getter
@Setter
@Validated
@ConfigurationProperties(prefix = "starrockswriter.schema-registry")
public class SchemaRegistryProperties {

  @NotBlank private String url;

  @Min(100)
  private int connectTimeoutMs = 3000;

  @Min(100)
  private int requestTimeoutMs = 5000;
}
