package com.nimbo.realtime.streamload;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbo.realtime.config.StarRocksProperties;
import com.nimbo.realtime.mapper.CsvFormatter;
import com.nimbo.realtime.mapper.SchemaMapping;
import com.nimbo.realtime.model.StarRocksRow;
import com.nimbo.realtime.model.StreamLoadResult;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.avro.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StarRocksStreamLoadClientTest {

    private HttpServer server;
    private String backendUrl;
    private StarRocksStreamLoadClient client;

    @BeforeEach
    void setup() throws Exception {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        backendUrl = "http://localhost:" + server.getAddress().getPort();

        StarRocksProperties properties = new StarRocksProperties();
        properties.setStreamLoadBackends(List.of(backendUrl));
        properties.setDatabase("test_db");
        properties.setTable("test_table");
        properties.setUsername("admin");
        properties.setPassword("secret");

        Schema schema = new Schema.Parser().parse("""
                {
                  "type":"record",
                  "name":"test",
                  "fields":[
                    {
                      "name":"value",
                      "type":"string"
                    }
                  ]
                }
                """);

        client = new StarRocksStreamLoadClient(new CsvFormatter(), properties, new ObjectMapper(),
                new BackendSelector(properties), new SchemaMapping(schema));
    }

    @AfterEach
    void cleanup() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void shouldSendStreamLoadRequestSuccessfully() throws Exception {
        server.createContext("/api/test_db/test_table/_stream_load", exchange -> {
            assertThat(exchange.getRequestMethod()).isEqualTo("PUT");
            assertThat(exchange.getRequestHeaders().getFirst("Authorization")).startsWith("Basic ");

            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            assertThat(body).contains("value");
            String response = """
                    {
                      "TxnId":1,
                      "Label":"label",
                      "Status":"Success",
                      "NumberLoadedRows":1
                    }
                    """;

            writeResponse(exchange, 200, response);
        });

        server.start();
        StreamLoadResult result = client.sendAsync(rows(), "label").join();

        assertThat(result.isSuccessful()).isTrue();
        assertThat(result.label()).isEqualTo("label");
    }

    @Test
    void shouldFailOnHttpError() {
        server.createContext("/api/test_db/test_table/_stream_load", exchange ->
                writeResponse(exchange, 500, "error"));

        server.start();
        assertThatThrownBy(() -> client.sendAsync(rows(), "label").join())
                .isInstanceOf(CompletionException.class);
    }

    @Test
    void shouldFailOnInvalidJsonResponse() {
        server.createContext("/api/test_db/test_table/_stream_load", exchange ->
                writeResponse(exchange, 200, "not-json"));

        server.start();
        assertThatThrownBy(() -> client.sendAsync(rows(), "label").join())
                .isInstanceOf(CompletionException.class);
    }

    @Test
    void shouldRejectEmptyBatch() {
        assertThatThrownBy(() -> client.sendAsync(List.of(), "label").join())
                .isInstanceOf(CompletionException.class);
    }

    private List<StarRocksRow> rows() {
        return List.of(new StarRocksRow("topic", 0, 1L, 2L, List.of("value")));
    }

    private void writeResponse(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }
}