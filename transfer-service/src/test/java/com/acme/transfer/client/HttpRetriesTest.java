package com.acme.transfer.client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.retry.Retry;

/**
 * INC-103: retries are classified instead of a blanket {@code retry(3)}. A 429 honours
 * {@code Retry-After}, a 5xx backs off, a 4xx is never retried, and the whole policy stops after a
 * bounded number of attempts. All the waiting happens in {@code Mono.delay}, so virtual time makes
 * the durations observable without a clock.
 */
class HttpRetriesTest {

  private static final int MAX_ATTEMPTS = 4;
  private static final Duration RETRY_AFTER_DEFAULT = Duration.ofSeconds(3);
  private static final Duration RETRY_AFTER_MAX = Duration.ofSeconds(30);
  private static final Duration MIN_BACKOFF = Duration.ofSeconds(1);

  private static WebClientResponseException http(int status, HttpHeaders headers) {
    return WebClientResponseException.create(status, "status " + status, headers, null, null);
  }

  private Retry policy() {
    return HttpRetries.classified(MAX_ATTEMPTS, RETRY_AFTER_DEFAULT, MIN_BACKOFF, RETRY_AFTER_MAX);
  }

  /** One failure of the given class, retried through the policy under test. */
  private Mono<Long> callRetrying(Throwable failure) {
    AtomicInteger attempts = new AtomicInteger();
    return Mono.<Long>defer(() -> {
          if (attempts.incrementAndGet() == 1) {
            return Mono.error(failure);
          }
          return Mono.just(1L);
        })
        .retryWhen(policy())
        .doOnNext(ignored -> assertEquals(2, attempts.get()));
  }

  @Test
  void aThrottling429WaitsForRetryAfter() {
    HttpHeaders headers = new HttpHeaders();
    headers.add("Retry-After", "5");

    StepVerifier.withVirtualTime(() -> callRetrying(http(429, headers)))
        .expectSubscription()
        .thenAwait(Duration.ofSeconds(5))
        .expectNext(1L)
        .verifyComplete();
  }

  @Test
  void a429WithoutRetryAfterWaitsForTheDocumentedDefault() {
    StepVerifier.withVirtualTime(() -> callRetrying(http(429, null)))
        .expectSubscription()
        .thenAwait(RETRY_AFTER_DEFAULT)
        .expectNext(1L)
        .verifyComplete();
  }

  @Test
  void aServerFailureBacksOffExponentially() {
    // first retry after 1s (minBackoff * 2^0), second after 2s (minBackoff * 2^1)
    AtomicInteger attempts = new AtomicInteger();
    Mono<Long> call = Mono.<Long>defer(() -> {
          if (attempts.incrementAndGet() <= 2) {
            return Mono.error(http(500, null));
          }
          return Mono.just(1L);
        })
        .retryWhen(policy());

    StepVerifier.withVirtualTime(() -> call)
        .expectSubscription()
        .thenAwait(Duration.ofSeconds(1))
        .thenAwait(Duration.ofSeconds(2))
        .expectNext(1L)
        .verifyComplete();
    assertEquals(3, attempts.get());
  }

  @Test
  void aClientFailureIsNotRetried() {
    StepVerifier.withVirtualTime(() -> callRetrying(http(404, null)))
        .expectSubscription()
        .thenAwait(Duration.ofSeconds(30))
        .expectError()
        .verify();
  }

  @Test
  void retriesAreBounded() {
    AtomicInteger attempts = new AtomicInteger();
    Mono<Long> alwaysBusy = Mono.<Long>defer(() -> {
          attempts.incrementAndGet();
          return Mono.error(http(429, null));
        })
        .retryWhen(policy());

    StepVerifier.withVirtualTime(() -> alwaysBusy)
        .expectSubscription()
        .thenAwait(Duration.ofSeconds(1000))
        .expectError()
        .verify();
    assertEquals(MAX_ATTEMPTS, attempts.get());
  }

  @Test
  void aReadTimeoutIsRetriedWithinTheBound() {
    StepVerifier.withVirtualTime(() -> callRetrying(new java.util.concurrent.TimeoutException()))
        .expectSubscription()
        .thenAwait(MIN_BACKOFF)
        .expectNext(1L)
        .verifyComplete();
  }

  @Test
  void aHostileRetryAfterIsCapped() {
    // The provider asks for an hour; no transfer waits longer than the configured cap.
    HttpHeaders headers = new HttpHeaders();
    headers.add("Retry-After", "3600");

    StepVerifier.withVirtualTime(() -> callRetrying(http(429, headers)))
        .expectSubscription()
        .thenAwait(RETRY_AFTER_MAX)
        .expectNext(1L)
        .verifyComplete();
  }

  @Test
  void retryAfterIsReadAsSeconds() {
    HttpHeaders headers = new HttpHeaders();
    headers.add("Retry-After", "7");
    assertEquals(7, HttpRetries.retryAfterSeconds(headers, 3));
  }

  @Test
  void anUnusableRetryAfterFallsBackToTheDefault() {
    HttpHeaders httpDate = new HttpHeaders();
    httpDate.add("Retry-After", "Wed, 21 Oct 2015 07:28:00 GMT");
    assertEquals(3, HttpRetries.retryAfterSeconds(httpDate, 3));

    assertEquals(3, HttpRetries.retryAfterSeconds(null, 3));

    HttpHeaders empty = new HttpHeaders();
    assertEquals(3, HttpRetries.retryAfterSeconds(empty, 3));

    HttpHeaders negative = new HttpHeaders();
    negative.add("Retry-After", "-5");
    assertEquals(0, HttpRetries.retryAfterSeconds(negative, 3));
  }
}
