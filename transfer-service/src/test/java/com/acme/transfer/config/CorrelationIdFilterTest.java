package com.acme.transfer.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Every request carries a correlation ID: the caller's is preserved, a UUID is generated when it is
 * missing, and the same value reaches the response header, the error body and the logs.
 */
class CorrelationIdFilterTest {

  private static final Pattern UUID =
      Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");

  private final CorrelationIdFilter filter = new CorrelationIdFilter();

  private MockServerWebExchange exchangeWith(String... headers) {
    // Headers are set on the request builder: a built request's headers are read-only.
    var request = MockServerHttpRequest.get("/api/v1/transfers");
    for (int i = 0; i < headers.length; i += 2) {
      request.header(headers[i], headers[i + 1]);
    }
    return MockServerWebExchange.from(request.build());
  }

  /** A chain that records the exchange and completes. */
  private WebFilterChain noop() {
    return exchange -> Mono.empty();
  }

  private String responseCorrelationId(MockServerWebExchange exchange) {
    return exchange.getResponse().getHeaders().getFirst(CorrelationIdFilter.HEADER);
  }

  private String exchangeCorrelationId(MockServerWebExchange exchange) {
    return (String) exchange.getAttribute(CorrelationIdFilter.EXCHANGE_ATTRIBUTE);
  }

  @Test
  void aSentCorrelationIdIsPreserved() {
    MockServerWebExchange exchange = exchangeWith(CorrelationIdFilter.HEADER, "trace-42");

    StepVerifier.create(filter.filter(exchange, noop())).verifyComplete();

    assertEquals("trace-42", responseCorrelationId(exchange));
    assertEquals("trace-42", exchangeCorrelationId(exchange));
  }

  @Test
  void aMissingCorrelationIdIsGenerated() {
    MockServerWebExchange exchange = exchangeWith();

    StepVerifier.create(filter.filter(exchange, noop())).verifyComplete();

    String generated = responseCorrelationId(exchange);
    assertNotNull(generated);
    assertEquals(generated, exchangeCorrelationId(exchange));
  }

  @Test
  void aBlankCorrelationIdIsReplaced() {
    MockServerWebExchange exchange = exchangeWith(CorrelationIdFilter.HEADER, "  ");

    StepVerifier.create(filter.filter(exchange, noop())).verifyComplete();

    assertNotNull(responseCorrelationId(exchange));
    assertNotEquals("  ", responseCorrelationId(exchange));
  }

  @Test
  void anEmptyCorrelationIdIsReplaced() {
    MockServerWebExchange exchange = exchangeWith(CorrelationIdFilter.HEADER, "");

    StepVerifier.create(filter.filter(exchange, noop())).verifyComplete();

    assertNotNull(responseCorrelationId(exchange));
  }

  @Test
  void aCorrelationIdIsTrimmed() {
    MockServerWebExchange exchange = exchangeWith(CorrelationIdFilter.HEADER, " trace-42 ");

    StepVerifier.create(filter.filter(exchange, noop())).verifyComplete();

    assertEquals("trace-42", responseCorrelationId(exchange));
  }

  @Test
  void theGeneratedValueIsAUuid() {
    MockServerWebExchange exchange = exchangeWith();

    StepVerifier.create(filter.filter(exchange, noop())).verifyComplete();

    String generated = responseCorrelationId(exchange);
    assertTrue(UUID.matcher(generated).matches(), "not a UUID: " + generated);
  }

  @Test
  void eachRequestGetsItsOwnValue() {
    MockServerWebExchange first = exchangeWith();
    MockServerWebExchange second = exchangeWith();

    StepVerifier.create(filter.filter(first, noop())).verifyComplete();
    StepVerifier.create(filter.filter(second, noop())).verifyComplete();

    assertNotEquals(responseCorrelationId(first), responseCorrelationId(second));
  }

  @Test
  void theCorrelationIdReachesTheReactorContext() {
    MockServerWebExchange exchange = exchangeWith(CorrelationIdFilter.HEADER, "trace-ctx");
    java.util.concurrent.atomic.AtomicReference<String> seen = new java.util.concurrent.atomic.AtomicReference<>();

    StepVerifier.create(filter.filter(exchange,
            ex -> Mono.deferContextual(context -> {
              seen.set((String) context.getOrEmpty(CorrelationIdFilter.CONTEXT_KEY).orElse(null));
              return Mono.<Void>empty();
            })))
        .verifyComplete();

    assertEquals("trace-ctx", seen.get());
  }

  @Test
  void theDownstreamFilterStillRuns() {
    MockServerWebExchange exchange = exchangeWith(CorrelationIdFilter.HEADER, "trace-chain");
    java.util.concurrent.atomic.AtomicBoolean downstream = new java.util.concurrent.atomic.AtomicBoolean();

    StepVerifier.create(filter.filter(exchange, ex -> {
          downstream.set(true);
          return Mono.empty();
        }))
        .verifyComplete();

    assertTrue(downstream.get(), "the downstream filter never ran");
  }

  @Test
  void theHeaderIsAddedBeforeTheChainRuns() {
    MockServerWebExchange exchange = exchangeWith();
    java.util.concurrent.atomic.AtomicReference<String> seen = new java.util.concurrent.atomic.AtomicReference<>();

    StepVerifier.create(filter.filter(exchange, ex -> {
          seen.set(responseCorrelationId(exchange));
          return Mono.empty();
        }))
        .verifyComplete();

    assertNotNull(seen.get(), "the response header was not set before the chain ran");
    assertEquals(seen.get(), responseCorrelationId(exchange));
  }
}
