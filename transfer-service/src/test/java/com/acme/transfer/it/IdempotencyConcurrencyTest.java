package com.acme.transfer.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.acme.transfer.support.MockBankContainer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.client.WebClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * INC-105 at the scale the incident happened at: one hundred callers send the very same request
 * with the very same idempotency key at the very same instant.
 *
 * <p>The old code did a SELECT and then an INSERT, so a hundred concurrent retries all observed
 * "missing" and each created its own transfer. This test proves the fix against the real database:
 * the unique key makes exactly one caller the owner, that owner posts once, and every other caller
 * is given the owner's stored answer rather than running the pipeline again. It reads the mock
 * bank's own record of what happened ({@code /__admin/stats}) because the point of the incident is
 * that what the API answers and what the dependency did can disagree.
 *
 * <p>The barrier is {@link Flux#merge} over one hundred lazily-built exchanges: nothing is
 * subscribed until merge subscribes, so all one hundred requests leave at the same moment, and the
 * pipeline completes only when the last one has been answered. Nothing sleeps, and nothing blocks
 * a thread to wait for another; the single {@code block} at the end is the test thread waiting on
 * the whole race, which is what a test thread is for.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers(disabledWithoutDocker = true)
class IdempotencyConcurrencyTest {

  private static final int CALLERS = 100;

  private static final String SOURCE = "2000000001";

  /** A second USD account, so the transfer stays same-currency and needs no FX call. */
  private static final String DESTINATION = "2000000002";

  private static final String BODY =
      "{\"sourceAccount\":\"" + SOURCE + "\",\"destinationAccount\":\"" + DESTINATION
          + "\",\"amount\":\"25.00\",\"currency\":\"USD\"}";

  @Container
  static final MockBankContainer MOCK_BANK = new MockBankContainer();

  @Container
  static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine")
      .withDatabaseName("transfers")
      .withUsername("transfers")
      .withPassword("transfers");

  @DynamicPropertySource
  static void urls(DynamicPropertyRegistry registry) {
    String bank = MOCK_BANK.baseUrl();
    String jdbc = "jdbc:postgresql://%s:%d/transfers".formatted(
        POSTGRES.getHost(), POSTGRES.getMappedPort(5432));
    String r2dbc = "r2dbc:postgresql://%s:%d/transfers".formatted(
        POSTGRES.getHost(), POSTGRES.getMappedPort(5432));
    registry.add("acme.accounts-url", () -> bank);
    registry.add("acme.fx-url", () -> bank);
    registry.add("acme.fraud-url", () -> bank);
    registry.add("acme.core-url", () -> bank);
    registry.add("spring.r2dbc.url", () -> r2dbc);
    registry.add("spring.r2dbc.username", POSTGRES::getUsername);
    registry.add("spring.r2dbc.password", POSTGRES::getPassword);
    registry.add("spring.flyway.url", () -> jdbc);
    registry.add("spring.flyway.user", POSTGRES::getUsername);
    registry.add("spring.flyway.password", POSTGRES::getPassword);
    // A scheduled pass must not touch this transfer while the test reads it.
    registry.add("acme.reconciliation.interval-seconds", () -> 3600);
  }

  @LocalServerPort
  private int port;

  private WebClient client;

  private final ObjectMapper json = new ObjectMapper();

  @BeforeEach
  void quiet() {
    MOCK_BANK.reset();
    MOCK_BANK.quiet();
    client = WebClient.builder().baseUrl("http://localhost:%d".formatted(port)).build();
  }

  @Test
  void oneHundredCallersShareOneKeyAndPostOnce() throws Exception {
    List<Integer> statuses = new CopyOnWriteArrayList<>();
    List<String> transferIds = new CopyOnWriteArrayList<>();
    List<Throwable> failures = new CopyOnWriteArrayList<>();

    // Each caller is built as a lazy exchange: no request leaves until the barrier subscribes.
    List<Mono<String>> callers = IntStream.range(0, CALLERS).mapToObj(i -> client
        .post().uri("/api/v1/transfers")
        .header("Idempotency-Key", "concurrent-race-001")
        .header("X-Correlation-Id", "race-" + i)
        .contentType(MediaType.APPLICATION_JSON)
        .bodyValue(BODY)
        .exchangeToMono(response -> response.bodyToMono(String.class).doOnNext(body -> {
          statuses.add(response.statusCode().value());
          try {
            transferIds.add(json.readTree(body).get("transferId").asText());
          } catch (Exception parseFailure) {
            failures.add(parseFailure);
          }
        }))
        .onErrorResume(failure -> {
          failures.add(failure);
          return Mono.empty();
        })).toList();

    // Subscribing to all one hundred at once is the barrier: every request leaves together, and
    // this returns only when the last caller has its answer.
    assertEquals(CALLERS,
        Flux.merge(callers).collectList().block(Duration.ofSeconds(90)).size(),
        "the barrier returned before every caller had answered");

    assertTrue(failures.isEmpty(),
        "a caller failed: " + (failures.isEmpty() ? "" : failures.get(0).getMessage()));
    assertEquals(CALLERS, statuses.size(), "every caller received an answer");
    assertEquals(CALLERS, transferIds.size(), "every caller received a body");

    // All one hundred see the same transfer in the same state: the owner's stored answer.
    assertEquals(1, Set.copyOf(transferIds).size(),
        "the callers saw " + Set.copyOf(transferIds).size() + " different transfers");
    assertEquals(Set.of(201), Set.copyOf(statuses),
        "the callers saw status codes " + Set.copyOf(statuses));

    JsonNode stats = json.readTree(MOCK_BANK.stats());
    assertEquals(1, stats.get("core").get("postings").get("total").asInt(),
        CALLERS + " callers, one core posting");
  }
}
