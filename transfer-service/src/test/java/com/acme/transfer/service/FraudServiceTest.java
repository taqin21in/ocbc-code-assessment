package com.acme.transfer.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.acme.transfer.client.FraudClient;
import com.acme.transfer.client.FraudDecision;
import com.acme.transfer.config.AcmeProperties;
import com.acme.transfer.dto.TransferRequest;
import java.math.BigDecimal;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * INC-104: fraud must fail closed. The vendor code turned an unreachable fraud service into
 * {@code ALLOW} and the transfer was posted anyway. Every failure here becomes
 * {@link FraudOutcome#UNAVAILABLE}, which the pipeline holds for review.
 */
class FraudServiceTest {

  private static final String TRANSFER_ID = "transfer-1";

  private FraudClient fraudClient;
  private FraudService fraudService;

  @BeforeEach
  void setUp() {
    fraudClient = mock(FraudClient.class);
    AcmeProperties properties = new AcmeProperties(
        "http://accounts", "http://fx", "http://fraud", "http://core",
        2000, 10, 3, Duration.ofMillis(100), Duration.ofSeconds(5), Duration.ofSeconds(5),
        Duration.ofMillis(1500),
        new AcmeProperties.Http(4, Duration.ofSeconds(1), Duration.ofSeconds(30), Duration.ofMillis(100)),
        new AcmeProperties.Reconciliation(30, 10),
        new AcmeProperties.FraudThreshold(new BigDecimal("75000000.00"), new BigDecimal("5000.00"),
            new BigDecimal("6500.00")),
        new AcmeProperties.Batch(1000, 8),
        new AcmeProperties.Sse(15));
    fraudService = new FraudService(fraudClient, properties);
  }

  private static TransferRequest request(String currency) {
    return new TransferRequest("2000000001", "2000000002", "100.00", currency, "lunch");
  }

  @Test
  void amountsBelowTheThresholdAreAllowedWithoutACall() {
    StepVerifier.create(fraudService.check(TRANSFER_ID, request("USD"), new BigDecimal("4999.99")))
        .expectNext(FraudOutcome.ALLOW)
        .verifyComplete();

    verify(fraudClient, never()).assess(anyString(), anyString(), anyString(), anyString(),
        anyString());
  }

  @Test
  void amountsAtTheThresholdAreScreened() {
    when(fraudClient.assess(anyString(), anyString(), anyString(), anyString(), anyString()))
        .thenReturn(Mono.just(new FraudDecision(TRANSFER_ID, "ALLOW", new BigDecimal("0.1"))));

    StepVerifier.create(fraudService.check(TRANSFER_ID, request("USD"), new BigDecimal("5000.00")))
        .expectNext(FraudOutcome.ALLOW)
        .verifyComplete();
  }

  @Test
  void thresholdsArePerCurrency() {
    when(fraudClient.assess(anyString(), anyString(), anyString(), anyString(), anyString()))
        .thenReturn(Mono.just(new FraudDecision(TRANSFER_ID, "DENY", new BigDecimal("0.9"))));

    StepVerifier.create(fraudService.check(TRANSFER_ID, request("IDR"), new BigDecimal("75000000.00")))
        .expectNext(FraudOutcome.DENY)
        .verifyComplete();

    StepVerifier.create(fraudService.check(TRANSFER_ID, request("SGD"), new BigDecimal("6500.00")))
        .expectNext(FraudOutcome.DENY)
        .verifyComplete();
  }

  @Test
  void reviewIsPassedThrough() {
    when(fraudClient.assess(anyString(), anyString(), anyString(), anyString(), anyString()))
        .thenReturn(Mono.just(new FraudDecision(TRANSFER_ID, "REVIEW", new BigDecimal("0.5"))));

    StepVerifier.create(fraudService.check(TRANSFER_ID, request("USD"), new BigDecimal("9999.00")))
        .expectNext(FraudOutcome.REVIEW)
        .verifyComplete();
  }

  /**
   * The heart of INC-104: a fraud service that is down, slow to answer, or speaking gibberish must
   * never become an allowance.
   */
  @Test
  void everyFraudFailureBecomesUnavailable() {
    when(fraudClient.assess(anyString(), anyString(), anyString(), anyString(), anyString()))
        .thenReturn(Mono.error(new java.util.concurrent.TimeoutException("fraud timed out")))
        .thenReturn(Mono.error(new IllegalStateException("connection refused")))
        .thenReturn(Mono.just(new FraudDecision(TRANSFER_ID, null, null)));

    StepVerifier.create(fraudService.check(TRANSFER_ID, request("USD"), new BigDecimal("9999.00")))
        .expectNext(FraudOutcome.UNAVAILABLE)
        .verifyComplete();
    StepVerifier.create(fraudService.check(TRANSFER_ID, request("USD"), new BigDecimal("9999.00")))
        .expectNext(FraudOutcome.UNAVAILABLE)
        .verifyComplete();
    StepVerifier.create(fraudService.check(TRANSFER_ID, request("USD"), new BigDecimal("9999.00")))
        .expectNext(FraudOutcome.UNAVAILABLE)
        .verifyComplete();
  }

  /**
   * A decision the fraud contract does not define is not an allowance: the service answered, but
   * not with anything we can act on, so the transfer is held for a human rather than posted
   * (INC-104: anything but a clear ALLOW may not move money).
   */
  @Test
  void anUnknownDecisionIsHeldForReview() {
    when(fraudClient.assess(anyString(), anyString(), anyString(), anyString(), anyString()))
        .thenReturn(Mono.just(new FraudDecision(TRANSFER_ID, "MAYBE", new BigDecimal("0.5"))));

    StepVerifier.create(fraudService.check(TRANSFER_ID, request("USD"), new BigDecimal("9999.00")))
        .expectNext(FraudOutcome.REVIEW)
        .verifyComplete();
  }

  @Test
  void theScreenedAmountIsComparedAsBigDecimal() {
    when(fraudClient.assess(anyString(), anyString(), anyString(), anyString(), anyString()))
        .thenReturn(Mono.just(new FraudDecision(TRANSFER_ID, "ALLOW", new BigDecimal("0.1"))));

    StepVerifier.create(fraudService.check(TRANSFER_ID, request("USD"), new BigDecimal("6000.00")))
        .expectNext(FraudOutcome.ALLOW)
        .verifyComplete();

    verify(fraudClient).assess(TRANSFER_ID, "6000.00", "USD", "2000000001", "2000000002");
  }
}
