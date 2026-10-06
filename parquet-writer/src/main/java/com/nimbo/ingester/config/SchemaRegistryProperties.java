package com.nimbo.ingester.config;

import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Getter
@Setter
@Validated
@ConfigurationProperties(prefix = "ingester.schema-registry")
public class SchemaRegistryProperties {

  @NotBlank private String url;
  @NotBlank private String subject;
}
