package com.nimbo.realtime.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.util.List;

@Getter
@Setter
@Validated
@ConfigurationProperties(prefix = "realtime.starrocks")
public class StarRocksProperties {

    @NotEmpty
    private List<@NotBlank String> streamLoadBackends;

    @NotBlank
    private String database;

    @NotBlank
    private String table;

    @NotBlank
    private String username;

    @NotNull
    private String password;

    @Min(100)
    private long retryBackoffMs;

    private boolean mergeCommitEnabled;
    private boolean mergeCommitAsync;

    @Min(1)
    private long mergeCommitIntervalMs;

    @Min(1)
    private int mergeCommitParallel;

    @Min(1)
    private int maxBatchSizeRows;

    @Min(1)
    private long maxBatchTimeMs;

    @Min(1)
    private int maxInFlightRequests;
}