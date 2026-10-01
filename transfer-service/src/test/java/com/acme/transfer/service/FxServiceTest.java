package com.acme.transfer.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.acme.transfer.client.FxClient;
import com.acme.transfer.client.FxRate;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * INC-106: FX conversion in {@link BigDecimal} end to end, rounded HALF_EVEN. A double-based
 * product of a large amount and an 8-decimal rate loses the cent; a stale rate is never quoted.
 */
class FxServiceTest {

  private final FxClient fxClient = mock(FxClient.class);
  private final FxService fxService = new FxService(fxClient);

  private static FxRate rate(String value, Instant validUntil) {
    return new FxRate("USD", "IDR", new BigDecimal(value), validUntil);
  }

  @Test
  void sameCurrencyNeedsNoRate() {
    StepVerifier.create(fxService.convert(new BigDecimal("150.00"), "USD", "USD"))
        .assertNext(conversion -> {
          assertEquals(new BigDecimal("150.00"), conversion.creditAmount());
          assertEquals("USD", conversion.creditCurrency());
          assertEquals(null, conversion.rate());
        })
        .verifyComplete();
  }

  @Test
  void conversionKeepsTheFullPrecisionProductRoundedToTwoPlaces() {
    when(fxClient.getRate("USD", "IDR"))
        .thenReturn(Mono.just(rate("15750.125", Instant.now().plus(30, ChronoUnit.SECONDS))));

    // 150.00 * 15750.125 = 2362518.7500 → two places, no rounding needed.
    StepVerifier.create(fxService.convert(new BigDecimal("150.00"), "USD", "IDR"))
        .assertNext(conversion -> {
          assertEquals(new BigDecimal("2362518.75"), conversion.creditAmount());
          assertEquals("IDR", conversion.creditCurrency());
          assertEquals(new BigDecimal("15750.125"), conversion.rate());
        })
        .verifyComplete();
  }

  @Test
  void aProductThatWouldRoundHalfwayGoesToTheEvenCent() {
    when(fxClient.getRate("USD", "IDR"))
        .thenReturn(Mono.just(rate("3", Instant.now().plus(30, ChronoUnit.SECONDS))));

    // 0.125 * 3 = 0.375 → the third decimal is exactly 5, so HALF_EVEN rounds to 0.38.
    StepVerifier.create(fxService.convert(new BigDecimal("0.125"), "USD", "IDR"))
        .assertNext(conversion ->
            assertEquals(new BigDecimal("0.38"), conversion.creditAmount().setScale(2)))
        .verifyComplete();
  }

  @Test
  void anExpiredRateIsNeverUsed() {
    when(fxClient.getRate("USD", "IDR"))
        .thenReturn(Mono.just(rate("15750.00", Instant.now().minus(5, ChronoUnit.SECONDS))));

    StepVerifier.create(fxService.convert(new BigDecimal("150.00"), "USD", "IDR"))
        .expectError(ExpiredRateException.class)
        .verify();
  }

  @Test
  void aRateWithoutValidityIsHonoured() {
    when(fxClient.getRate("USD", "IDR")).thenReturn(Mono.just(rate("15000.00", null)));

    StepVerifier.create(fxService.convert(new BigDecimal("2.00"), "USD", "IDR"))
        .assertNext(conversion ->
            assertEquals(new BigDecimal("30000.00"), conversion.creditAmount().setScale(2)))
        .verifyComplete();
  }

  @Test
  void aRateThatCannotBeFetchedFailsTheTransfer() {
    when(fxClient.getRate(any(), any())).thenReturn(Mono.error(new IllegalStateException("down")));

    StepVerifier.create(fxService.convert(new BigDecimal("150.00"), "USD", "IDR"))
        .expectError(IllegalStateException.class)
        .verify();
  }
}
