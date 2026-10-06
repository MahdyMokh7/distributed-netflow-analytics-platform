package com.nimbo.starrockswriter.support;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import org.apache.avro.Schema;

public final class TestSchemaRegistry implements AutoCloseable {

  public static final int SCHEMA_ID = 1;

  private static final String SCHEMA_JSON =
      """
      {
        "type":"record",
        "name":"TestNetFlowRecord",
        "namespace":"com.nimbo.netflow",
        "fields":[
          {"name":"IPV4_SRC_ADDR","type":"string"},
          {"name":"IPV4_DST_ADDR","type":"string"},
          {"name":"IN_BYTES","type":"long"}
        ]
      }
      """;

  private HttpServer server;

  public synchronized void start() {
    if (server != null) {
      return;
    }
    try {
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      server.createContext("/schemas/ids/" + SCHEMA_ID, this::schemaResponse);
      server.start();
    } catch (IOException exception) {
      throw new IllegalStateException("Failed to start test Schema Registry", exception);
    }
  }

  public String url() {
    if (server == null) {
      throw new IllegalStateException("Test Schema Registry is not started");
    }
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }

  public Schema schema() {
    return new Schema.Parser().parse(SCHEMA_JSON);
  }

  private void schemaResponse(HttpExchange exchange) throws IOException {
    if (!"GET".equals(exchange.getRequestMethod())) {
      exchange.sendResponseHeaders(405, -1);
      exchange.close();
      return;
    }

    String escaped = SCHEMA_JSON.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    byte[] body =
        ("{\"schemaType\":\"AVRO\",\"schema\":\"" + escaped + "\"}")
            .getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(200, body.length);
    exchange.getResponseBody().write(body);
    exchange.close();
  }

  @Override
  public synchronized void close() {
    if (server != null) {
      server.stop(0);
      server = null;
    }
  }
}
