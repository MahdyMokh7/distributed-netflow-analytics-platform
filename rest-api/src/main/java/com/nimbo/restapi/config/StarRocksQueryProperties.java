package com.nimbo.restapi.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

@Getter
@Setter
@Component
@Validated
@ConfigurationProperties(prefix = "nimbo.starrocks-query")
public class StarRocksQueryProperties {

  @NotBlank private String jdbcUrl;

  @NotBlank private String username;

  @NotNull private String password;

  @NotBlank private String database;

  @NotBlank private String table;

  @Min(1)
  private int maximumPoolSize = 10;

  @Min(1)
  private int queryTimeoutSeconds = 30;

  @Min(1)
  private int maxRows = 1000;

  public String qualifiedTable() {
    validateIdentifier(database);
    validateIdentifier(table);

    return "`" + database + "`.`" + table + "`";
  }

  private void validateIdentifier(String value) {
    if (!value.matches("[A-Za-z0-9_]+")) {
      throw new IllegalArgumentException("Invalid StarRocks identifier: " + value);
    }
  }
}
