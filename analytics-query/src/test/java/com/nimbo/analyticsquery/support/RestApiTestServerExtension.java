package com.nimbo.analyticsquery.support;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public final class RestApiTestServerExtension
    implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback, AfterAllCallback {

  private final AtomicInteger requestCount = new AtomicInteger();
  private final ConcurrentLinkedQueue<ConfiguredResponse> responses = new ConcurrentLinkedQueue<>();

  private HttpServer server;
  private volatile ConfiguredResponse response;
  private volatile URI lastRequestUri;
  private volatile CountDownLatch responseRelease = new CountDownLatch(0);

  @Override
  public void beforeAll(ExtensionContext context) {
    ensureStarted();
  }

  @Override
  public void beforeEach(ExtensionContext context) {
    releaseResponse();
    responseRelease = new CountDownLatch(0);
    requestCount.set(0);
    lastRequestUri = null;
    responses.clear();
    respondWith(
        500,
        """
        {
          "success": false,
          "message": "No test response configured"
        }
        """);
  }

  @Override
  public void afterEach(ExtensionContext context) {
    releaseResponse();
  }

  @Override
  public void afterAll(ExtensionContext context) {
    if (server != null) {
      server.stop(0);
    }
  }

  public URI baseUrl() {
    ensureStarted();
    return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/api/");
  }

  public URI originUrl() {
    ensureStarted();
    return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
  }

  public int requestCount() {
    return requestCount.get();
  }

  public URI lastRequestUri() {
    return lastRequestUri;
  }

  public void respondWith(int status, String body) {
    response = new ConfiguredResponse(status, body.getBytes(StandardCharsets.UTF_8));
  }

  public void respondWithSequence(String... bodies) {
    responses.clear();
    for (String body : bodies) {
      responses.add(new ConfiguredResponse(200, body.getBytes(StandardCharsets.UTF_8)));
    }
  }

  public void holdResponse() {
    responseRelease = new CountDownLatch(1);
  }

  public void releaseResponse() {
    responseRelease.countDown();
  }

  private void respond(HttpExchange exchange) throws IOException {
    requestCount.incrementAndGet();
    lastRequestUri = exchange.getRequestURI();
    CountDownLatch release = responseRelease;
    ConfiguredResponse configured = responses.poll();
    if (configured == null) {
      configured = response;
    }

    try {
      release.await(5, TimeUnit.SECONDS);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      exchange.close();
      return;
    }

    int status = configured.status();
    byte[] responseBytes = configured.body();

    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, responseBytes.length);
    try (var body = exchange.getResponseBody()) {
      body.write(responseBytes);
    } finally {
      exchange.close();
    }
  }

  private synchronized void ensureStarted() {
    if (server != null) {
      return;
    }

    try {
      server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
      server.createContext("/api/", this::respond);
      server.start();
    } catch (IOException exception) {
      throw new UncheckedIOException("Failed to start the test HTTP server", exception);
    }
  }

  private record ConfiguredResponse(int status, byte[] body) {}
}
