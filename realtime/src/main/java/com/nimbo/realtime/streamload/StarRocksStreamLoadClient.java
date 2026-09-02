package com.nimbo.realtime.streamload;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbo.realtime.config.StarRocksProperties;
import com.nimbo.realtime.mapper.CsvFormatter;
import com.nimbo.realtime.mapper.SchemaMapping;
import com.nimbo.realtime.model.StarRocksRow;
import com.nimbo.realtime.model.StreamLoadResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

@Slf4j
@Component
@RequiredArgsConstructor
public class StarRocksStreamLoadClient {
    private final CsvFormatter csvFormatter;
    private final StarRocksProperties starRocksProperties;
    private final ObjectMapper objectMapper;
    private final BackendSelector backendSelector;
    private final SchemaMapping schemaMapping;
    private final HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5))
            .followRedirects(HttpClient.Redirect.NEVER).build();

    private HttpRequest buildRequest(URI uri, String csvBody, String label) {
        HttpRequest.Builder builder = HttpRequest.newBuilder().uri(uri).timeout(Duration.ofSeconds(60))
                .header("Authorization", buildAuthorizationHeader())
                .header("format", "CSV")
                .header("column_separator", ",")
                .header("enclose", "\"")
                .header("columns", schemaMapping.starRocksColumns())
                .header("strict_mode", "true")
                .header("max_filter_ratio", "0")
                .header("Content-Type", "text/csv")
                .expectContinue(true);

        if (starRocksProperties.isMergeCommitEnabled()) {
            builder.header("enable_merge_commit", "true");
            builder.header("merge_commit_async", Boolean.toString(starRocksProperties.isMergeCommitAsync()));
            builder.header("merge_commit_interval_ms", Long.toString(starRocksProperties.getMergeCommitIntervalMs()));
            builder.header("merge_commit_parallel", Integer.toString(starRocksProperties.getMergeCommitParallel()));
        } else {
            builder.header("label", label);
        }
        return builder.PUT(HttpRequest.BodyPublishers.ofString(csvBody, StandardCharsets.UTF_8)).build();
    }

    private String buildStreamLoadUrl() {
        String backend = backendSelector.nextBackend();
        if (backend.endsWith("/")) backend = backend.substring(0, backend.length() - 1);
        return backend + "/api/" + starRocksProperties.getDatabase() + "/" + starRocksProperties.getTable() + "/_stream_load";
    }

    public CompletableFuture<StreamLoadResult> sendAsync(List<StarRocksRow> rows, String label) {
        if (rows == null || rows.isEmpty()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Stream Load batch cannot be empty"));
        }

        URI uri = URI.create(buildStreamLoadUrl());
        HttpRequest request = buildRequest(uri, csvFormatter.format(rows), label);
        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
                .thenApply(response -> {
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new CompletionException(new IOException("StarRocks Stream Load HTTP failure. status="
                        + response.statusCode() + ", body=" + response.body()));
            }
            try {
                return objectMapper.readValue(response.body(), StreamLoadResult.class);
            } catch (IOException e) {
                throw new CompletionException(e);
            }
        });
    }

    private String buildAuthorizationHeader() {
        String credentials = starRocksProperties.getUsername() + ":" + starRocksProperties.getPassword();
        return "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
    }
}
