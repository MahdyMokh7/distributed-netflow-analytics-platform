package com.nimbo.starrockswriter.streamload;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nimbo.starrockswriter.config.StarRocksProperties;
import com.nimbo.starrockswriter.mapper.CsvFormatter;
import com.nimbo.starrockswriter.mapper.SchemaMapping;
import com.nimbo.starrockswriter.model.StarRocksRow;
import com.nimbo.starrockswriter.model.StreamLoadResult;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.avro.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.nimbo.starrockswriter.metrics.StarRocksWriterMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class StreamLoadRetryHandlerTest {

    private HttpServer server;
    private AtomicInteger requestCount;
    private StreamLoadRetryHandler retryHandler;

    @BeforeEach
    void setup() throws Exception {
        requestCount = new AtomicInteger();

        server = HttpServer.create(new InetSocketAddress(0), 0);

        String backend = "http://localhost:" + server.getAddress().getPort();

        StarRocksProperties properties = new StarRocksProperties();
        properties.setStreamLoadBackends(List.of(backend));
        properties.setDatabase("test_db");
        properties.setTable("test_table");
        properties.setUsername("user");
        properties.setPassword("password");

        // keep tests fast
        properties.setRetryBackoffMs(1);

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

        StarRocksStreamLoadClient client = new StarRocksStreamLoadClient(
                new CsvFormatter(),
                properties,
                new ObjectMapper(),
                new BackendSelector(properties),
                new SchemaMapping(schema)
        );

        StarRocksWriterMetrics metrics = new StarRocksWriterMetrics(new SimpleMeterRegistry());
        metrics.registerWorker(1);
        retryHandler = new StreamLoadRetryHandler(properties, client, 1, metrics);
    }

    @AfterEach
    void cleanup() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void shouldReturnImmediatelyWhenFirstRequestSucceeds() throws Exception {
        server.createContext("/api/test_db/test_table/_stream_load", exchange -> {
            requestCount.incrementAndGet();
            respond(exchange, """
                    {
                      "Status":"Success",
                      "Label":"label"
                    }
                    """);
        });

        server.start();

        StreamLoadResult result = retryHandler.sendRequest(rows(), "label").join();

        assertThat(result.isSuccessful()).isTrue();
        assertThat(requestCount.get()).isEqualTo(1);
    }

    @Test
    void shouldRetryAfterFailedRequest() throws Exception {
        server.createContext("/api/test_db/test_table/_stream_load", exchange -> {
            int attempt = requestCount.incrementAndGet();

            if (attempt == 1) {
                respond(exchange, 500, "failed");
            } else {
                respond(exchange, """
                        {
                          "Status":"Success",
                          "Label":"label"
                        }
                        """);
            }
        });

        server.start();

        StreamLoadResult result = retryHandler.sendRequest(rows(), "label").join();

        assertThat(result.isSuccessful()).isTrue();
        assertThat(requestCount.get()).isEqualTo(2);
    }

    @Test
    void shouldAcceptPublishTimeoutAsSuccess() throws Exception {
        server.createContext("/api/test_db/test_table/_stream_load", exchange ->
                respond(exchange, """
                        {
                          "Status":"Publish Timeout",
                          "Label":"label"
                        }
                        """)
        );

        server.start();

        StreamLoadResult result = retryHandler.sendRequest(rows(), "label").join();

        assertThat(result.status()).isEqualTo("Publish Timeout");
    }

    private List<StarRocksRow> rows() {
        return List.of(new StarRocksRow("topic", 0, 1L, 2L, List.of("value")));
    }

    private void respond(HttpExchange exchange, String body) throws IOException {
        respond(exchange, 200, body);
    }

    private void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }
}
