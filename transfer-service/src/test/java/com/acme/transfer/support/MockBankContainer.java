package com.acme.transfer.support;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

/**
 * The mock bank in a container, built from {@code ../mock-bank}, for integration tests.
 *
 * <pre>{@code
 * @Testcontainers
 * class SomethingIntegrationTest {
 *
 *   @Container
 *   static final MockBankContainer MOCK_BANK = new MockBankContainer();
 *
 *   @BeforeEach
 *   void quietMockBank() {
 *     MOCK_BANK.reset();
 *     MOCK_BANK.quiet();
 *   }
 * }
 * }</pre>
 *
 * <p>Point the service's accounts, FX, fraud and core URLs at {@link #baseUrl()}. The admin
 * helpers below follow {@code docs/integration/mock-bank-admin.md}.
 */
public class MockBankContainer extends GenericContainer<MockBankContainer> {

  public static final int PORT = 9090;

  /** No latency and no random failures. */
  public static final String QUIET = """
      {"accounts": {"latencyMs": {"min": 0, "max": 0}, "errorRate": 0},
       "fx": {"latencyMs": {"min": 0, "max": 0}, "errorRate": 0},
       "fraud": {"latencyMs": {"min": 0, "max": 0}, "errorRate": 0, "slowRate": 0},
       "core": {"latencyMs": {"min": 0, "max": 0}, "hangRate": 0,
                "inquiryLatencyMs": {"min": 0, "max": 0}}}
      """;

  private final HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();

  public MockBankContainer() {
    super(new ImageFromDockerfile("acme-mock-bank-test", false)
        .withFileFromPath(".", Path.of("..", "mock-bank")));
    withExposedPorts(PORT);
    waitingFor(Wait.forHttp("/health").forStatusCode(200));
  }

  public String baseUrl() {
    return "http://" + getHost() + ":" + getMappedPort(PORT);
  }

  /** Clears state and statistics; default chaos, no overrides. */
  public void reset() {
    send("POST", "/__admin/reset", null);
  }

  /** No latency and no random failures, for deterministic tests. */
  public void quiet() {
    chaos(QUIET);
  }

  public void chaos(String json) {
    send("PUT", "/__admin/chaos", json);
  }

  public void overrides(String json) {
    send("PUT", "/__admin/overrides", json);
  }

  /** What each service saw, and every core posting, as JSON. */
  public String stats() {
    return send("GET", "/__admin/stats", null);
  }

  private String send(String method, String path, String json) {
    HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(baseUrl() + path))
        .timeout(Duration.ofSeconds(10));
    if (json == null) {
      request.method(method, HttpRequest.BodyPublishers.noBody());
    } else {
      request.method(method, HttpRequest.BodyPublishers.ofString(json))
          .header("Content-Type", "application/json");
    }
    try {
      HttpResponse<String> response = http.send(request.build(), HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() >= 400) {
        throw new IllegalStateException(
            method + " " + path + " answered " + response.statusCode() + ": " + response.body());
      }
      return response.body();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }
}
