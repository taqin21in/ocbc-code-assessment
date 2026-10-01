package com.acme.transfer.client;

import com.acme.transfer.config.AcmeProperties;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

/**
 * Retry classification for outbound HTTP calls, one policy per failure class instead of a blanket
 * {@code retry(3)}:
 *
 * <pre>
 * 429  → wait the number of seconds in Retry-After, then retry (bounded)
 * 5xx  → bounded exponential backoff
 * 4xx  → do not retry: the request is wrong, repeating it cannot help
 * </pre>
 *
 * <p>The {@code Retry-After} header is honoured to the letter, and when it is missing the
 * documented safe default from {@link AcmeProperties.Fx#retryAfterDefault()} is used instead.
 * Delays are {@link Mono#delay(Duration)}, never {@code Thread.sleep}, so tests can run them in
 * virtual time.
 */
final class HttpRetries {

  private HttpRetries() {
  }

  /**
   * A policy that retries at most {@code maxAttempts - 1} times and classifies the failure of each
   * attempt. Read operations only: a retry always re-sends a request that can be repeated.
   */
  static Retry classified(int maxAttempts, Duration retryAfterDefault, Duration minBackoff,
                          Duration retryAfterMax) {
    return Retry.from(signals -> signals.flatMap(signal -> {
      Throwable failure = signal.failure();
      if (signal.totalRetries() >= maxAttempts - 1) {
        return Mono.error(failure);
      }
      if (failure instanceof TimeoutException) {
        // A read that never answered: nothing was committed, try again within the bound.
        return Mono.delay(exponential(minBackoff, signal.totalRetries()));
      }
      if (failure instanceof WebClientResponseException response) {
        int status = response.getStatusCode().value();
        if (status == 429) {
          long seconds = Math.min(
              retryAfterSeconds(response.getHeaders(), (int) retryAfterDefault.toSeconds()),
              retryAfterMax.toSeconds());
          return Mono.delay(Duration.ofSeconds(seconds));
        }
        if (status >= 500) {
          return Mono.delay(exponential(minBackoff, signal.totalRetries()));
        }
      }
      // Non-retryable: 4xx other than a throttling 429, or a failure the caller must see.
      return Mono.error(failure);
    }));
  }

  private static Duration exponential(Duration minBackoff, long attempt) {
    return minBackoff.multipliedBy(1L << Math.min(attempt, 30));
  }

  /** The {@code Retry-After} header in seconds, or the documented default when it is unusable. */
  static long retryAfterSeconds(HttpHeaders headers, int defaultSeconds) {
    String value = headers == null ? null : headers.getFirst("Retry-After");
    if (value != null && !value.isBlank()) {
      try {
        return Math.max(Integer.parseInt(value.trim()), 0);
      } catch (NumberFormatException ignored) {
        // HTTP dates are not supported: fall back to the safe default.
      }
    }
    return defaultSeconds;
  }
}
