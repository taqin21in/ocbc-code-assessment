package com.acme.transfer.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.acme.transfer.config.AcmeProperties;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;
import reactor.test.StepVerifier;

/**
 * The outbound clients against a real HTTP server, so the retry classification, the timeouts and
 * the 404-to-empty mapping are exercised over the wire instead of against a mock.
 */
class ClientHttpTest {

  private DisposableServer server;
  private AcmeProperties properties;
  private final AtomicInteger hits = new AtomicInteger();

  @BeforeEach
  void setUp() {
    server = HttpServer.create()
        .host("127.0.0.1")
        .port(0)
        .route(routes -> routes
            .get("/accounts/{accountNo}", (request, response) -> {
              hits.incrementAndGet();
              String accountNo = request.param("accountNo");
              if ("4040000000".equals(accountNo)) {
                return response.status(404).sendString(Mono.just("not found"));
              }
              if ("5000000000".equals(accountNo)) {
                return response.status(500).sendString(Mono.just("boom"));
              }
              return response.header("Content-Type", "application/json")
                  .sendString(Mono.just(
                      "{\"accountNo\":\"" + accountNo + "\",\"name\":\"Test\",\"currency\":\"USD\","
                          + "\"status\":\"ACTIVE\",\"available\":100000.00}"));
            })
            .get("/fx/rates", (request, response) -> {
              hits.incrementAndGet();
              // Reactor Netty's param() is a path variable; query parameters are on the URI.
              String base = queryParam(request, "base");
              if ("THROTTLED".equals(base)) {
                return response.status(429)
                    .header("Retry-After", "0")
                    .sendString(Mono.just("too many"));
              }
              if ("BROKEN".equals(base)) {
                return response.status(400).sendString(Mono.just("no such currency"));
              }
              return response.header("Content-Type", "application/json")
                  .sendString(Mono.just(
                      "{\"base\":\"USD\",\"quote\":\"IDR\",\"rate\":15000.00,"
                          + "\"validUntil\":\"2099-01-01T00:00:00Z\"}"));
            })
            .post("/fraud/assessments", (request, response) -> {
              hits.incrementAndGet();
              return response.header("Content-Type", "application/json")
                  .sendString(Mono.just(
                      "{\"transferId\":\"transfer-1\",\"decision\":\"ALLOW\",\"score\":0.1}"));
            }))
        .bindNow();

    properties = new AcmeProperties(
        "http://127.0.0.1:" + server.port(), "http://127.0.0.1:" + server.port(),
        "http://127.0.0.1:" + server.port(), "http://127.0.0.1:" + server.port(),
        2000, 10, 3, Duration.ofMillis(100), Duration.ofSeconds(5), Duration.ofSeconds(5),
        Duration.ofSeconds(2),
        new AcmeProperties.Http(4, Duration.ofSeconds(1), Duration.ofSeconds(30), Duration.ofMillis(100)),
        new AcmeProperties.Reconciliation(30, 10),
        new AcmeProperties.FraudThreshold(new BigDecimal("75000000.00"), new BigDecimal("5000.00"),
            new BigDecimal("6500.00")),
        new AcmeProperties.Batch(1000, 8),
        new AcmeProperties.Sse(15));
  }

  @AfterEach
  void tearDown() {
    server.disposeNow();
  }

  /** Reads one query parameter of the request URI; reactor-netty's own param() is path variables. */
  private static String queryParam(reactor.netty.http.server.HttpServerRequest request,
                                   String name) {
    return new io.netty.handler.codec.http.QueryStringDecoder(request.uri()).parameters()
        .getOrDefault(name, java.util.List.of()).stream().findFirst().orElse(null);
  }

  @Test
  void anAccountIsReadAsJson() {
    AccountClient client = new AccountClient(WebClient.builder(), properties);

    StepVerifier.create(client.getAccount("2000000001"))
        .assertNext(account -> {
          assertEquals("2000000001", account.accountNo());
          assertEquals("USD", account.currency());
          assertEquals("ACTIVE", account.status());
        })
        .verifyComplete();
  }

  @Test
  void aMissingAccountIsEmptyNotAnError() {
    AccountClient client = new AccountClient(WebClient.builder(), properties);

    StepVerifier.create(client.getAccount("4040000000"))
        .verifyComplete();
  }

  @Test
  void anAccountErrorIsPropagated() {
    AccountClient client = new AccountClient(WebClient.builder(), properties);

    StepVerifier.create(client.getAccount("5000000000"))
        .expectError()
        .verify();
  }

  @Test
  void aRateIsReadAsJson() {
    FxClient client = new FxClient(WebClient.builder(), properties);

    StepVerifier.create(client.getRate("USD", "IDR"))
        .assertNext(rate -> {
          assertEquals("USD", rate.base());
          assertEquals("IDR", rate.quote());
          assertEquals(new BigDecimal("15000.00"), rate.rate());
        })
        .verifyComplete();
  }

  /**
   * INC-103: a 429 is retried, honouring Retry-After (the server sends 0 so the test is
   * deterministic), and the attempts are bounded — the provider cannot stall us forever.
   */
  @Test
  void a429IsRetriedUntilTheAttemptsRunOut() {
    hits.set(0);
    FxClient client = new FxClient(WebClient.builder(), properties);

    StepVerifier.create(client.getRate("THROTTLED", "IDR"))
        .expectError()
        .verify();

    assertEquals(4, hits.get(), "one call plus three bounded retries");
  }

  @Test
  void a400IsNotRetried() {
    hits.set(0);
    FxClient client = new FxClient(WebClient.builder(), properties);

    StepVerifier.create(client.getRate("BROKEN", "IDR"))
        .expectError()
        .verify();

    assertEquals(1, hits.get(), "a client error must not be retried");
  }

  @Test
  void aFraudAssessmentIsPostedAndRead() {
    FraudClient client = new FraudClient(WebClient.builder(), properties);

    StepVerifier.create(client.assess("transfer-1", "100.00", "USD", "2000000001", "2000000002"))
        .assertNext(decision -> {
          assertEquals("transfer-1", decision.transferId());
          assertEquals("ALLOW", decision.decision());
        })
        .verifyComplete();
  }
}
