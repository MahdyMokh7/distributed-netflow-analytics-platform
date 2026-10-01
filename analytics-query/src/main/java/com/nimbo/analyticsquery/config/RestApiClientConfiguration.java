package com.nimbo.analyticsquery.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

import java.net.http.HttpClient;

@Configuration
public class RestApiClientConfiguration {

    @Bean
    @Qualifier("restApiRestClient")
    public RestClient restApiRestClient(RestClient.Builder builder, RestApiProperties properties) {

        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(properties.timeout().connect())
                .followRedirects(HttpClient.Redirect.NEVER)
                .build();

        JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(httpClient);

        requestFactory.setReadTimeout(properties.timeout().read());

        return builder
                .baseUrl(normalizeBaseUrl(properties.baseUrl()))
                .defaultHeader(
                        HttpHeaders.ACCEPT,
                        MediaType.APPLICATION_JSON_VALUE)
                .requestFactory(requestFactory)
                .build();
    }

    private String normalizeBaseUrl(java.net.URI baseUrl) {
        String value = baseUrl.toString();

        return value.endsWith("/") ? value : value + "/";
    }
}