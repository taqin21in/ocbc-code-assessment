package com.acme.transfer.kit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.util.retry.Retry;

/**
 * Worked {@code StepVerifier} examples on a toy thermometer. Nothing here is about transfers: take
 * the techniques, not the code.
 */
class StepVerifierExamplesTest {

  /** A toy thermometer that asks an unreliable sensor for readings. */
  static final class Thermometer {

    private final Mono<Integer> sensor;

    Thermometer(Mono<Integer> sensor) {
      this.sensor = sensor;
    }

    /** One reading. Each attempt gives up after 2 s; failures are retried twice, after 1 s and 2 s. */
    Mono<Integer> read() {
      return sensor
          .timeout(Duration.ofSeconds(2))
          .retryWhen(Retry.backoff(2, Duration.ofSeconds(1)).jitter(0));
    }

    /** A reading every minute. */
    Flux<Integer> watch() {
      return Flux.interval(Duration.ofMinutes(1)).concatMap(tick -> read());
    }
  }

  @Test
  void aValueThenCompletion() {
    StepVerifier.create(new Thermometer(Mono.just(21)).read())
        .expectNext(21)
        .verifyComplete();
  }

  @Test
  void anError() {
    StepVerifier.create(Mono.error(new IllegalStateException("sensor unplugged")))
        .expectErrorMessage("sensor unplugged")
        .verify();
  }

  @Test
  void aTimeoutInVirtualTime() {
    StepVerifier.withVirtualTime(() -> Mono.never().timeout(Duration.ofSeconds(2)))
        .expectSubscription()
        .thenAwait(Duration.ofSeconds(2))
        .expectError(TimeoutException.class)
        .verify();
  }

  @Test
  void retriesWithBackoffInVirtualTime() {
    AtomicInteger attempts = new AtomicInteger();
    Mono<Integer> flaky = Mono.defer(() -> attempts.incrementAndGet() < 3
        ? Mono.error(new IllegalStateException("glitch"))
        : Mono.just(19));

    // Must be created inside the supplier, so its delays run on virtual time.
    StepVerifier.withVirtualTime(() -> new Thermometer(flaky).read())
        .expectSubscription()
        .expectNoEvent(Duration.ofMillis(2999))
        .thenAwait(Duration.ofMillis(1))
        .expectNext(19)
        .verifyComplete();
    assertEquals(3, attempts.get());
  }

  @Test
  void givesUpAfterTheLastRetry() {
    StepVerifier.withVirtualTime(() -> new Thermometer(Mono.never()).read())
        .expectSubscription()
        // three attempts of 2 s each, plus 1 s and 2 s of backoff
        .thenAwait(Duration.ofSeconds(9))
        .expectErrorMatches(Exceptions::isRetryExhausted)
        .verify();
  }

  @Test
  void hoursOfStreamingInMilliseconds() {
    StepVerifier.withVirtualTime(() -> new Thermometer(Mono.just(20)).watch().take(3))
        .expectSubscription()
        .thenAwait(Duration.ofMinutes(3))
        .expectNext(20, 20, 20)
        .verifyComplete();
  }

  @Test
  void decideWhenValuesArriveWithATestPublisher() {
    TestPublisher<Integer> sensor = TestPublisher.create();

    StepVerifier.create(new Thermometer(sensor.mono()).read())
        .then(sensor::assertWasSubscribed)
        .then(() -> sensor.emit(18))
        .expectNext(18)
        .verifyComplete();
  }

  @Test
  void requestOnlyWhatYouNeed() {
    StepVerifier.create(Flux.range(1, 10), 0)
        .thenRequest(3)
        .expectNext(1, 2, 3)
        .thenCancel()
        .verify();
  }

  @Test
  void cancellingReachesTheSource() {
    AtomicBoolean cancelled = new AtomicBoolean();

    StepVerifier.create(Flux.<Integer>never().doOnCancel(() -> cancelled.set(true)))
        .expectSubscription()
        .thenCancel()
        .verify();
    assertTrue(cancelled.get());
  }
}
