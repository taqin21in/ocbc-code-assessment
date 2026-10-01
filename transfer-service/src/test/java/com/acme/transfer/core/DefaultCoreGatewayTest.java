package com.acme.transfer.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.acme.core.sdk.CoreBankingClient;
import com.acme.core.sdk.CoreBusyException;
import com.acme.core.sdk.CoreTimeoutException;
import com.acme.core.sdk.CoreUnavailableException;
import com.acme.core.sdk.PostingRequest;
import com.acme.core.sdk.PostingResult;
import com.acme.transfer.config.AcmeProperties;
import com.acme.transfer.kit.VirtualThreads;
import com.acme.transfer.kit.VirtualThreads.ConcurrencyProbe;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

/**
 * INC-102 / INC-108: the core banking gateway runs the blocking SDK on virtual threads behind one
 * shared semaphore, and never retries a request whose outcome is unknown.
 */
class DefaultCoreGatewayTest {

  private static final int MAX_SESSIONS = 10;

  private ExecutorService virtualThreadExecutor;
  private CoreBankingClient client;
  private DefaultCoreGateway gateway;

  @BeforeEach
  void setUp() {
    virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
    client = mock(CoreBankingClient.class);
    AcmeProperties properties = new AcmeProperties(
        "http://accounts", "http://fx", "http://fraud", "http://core",
        2000, MAX_SESSIONS, 3, Duration.ofMillis(1), Duration.ofSeconds(5), Duration.ofSeconds(5),
        Duration.ofMillis(1500),
        new AcmeProperties.Http(4, Duration.ofSeconds(1), Duration.ofSeconds(30), Duration.ofMillis(100)),
        new AcmeProperties.Reconciliation(30, 10),
        new AcmeProperties.FraudThreshold(new BigDecimal("75000000.00"),
            new BigDecimal("5000.00"), new BigDecimal("6500.00")),
        new AcmeProperties.Batch(1000, 8),
        new AcmeProperties.Sse(15));
    gateway = new DefaultCoreGateway(client, properties, virtualThreadExecutor);
  }

  @AfterEach
  void tearDown() {
    virtualThreadExecutor.shutdownNow();
  }

  private static PostingRequest request() {
    return new PostingRequest("transfer-1", "2000000001", "2000000002", new BigDecimal("10.00"),
        "USD", new BigDecimal("10.00"), "USD");
  }

  private static PostingResult posted() {
    return new PostingResult("transfer-1", "CT1", PostingResult.Status.POSTED, null, null);
  }

  @Test
  void aPostingRunsOnAVirtualThread() {
    when(client.post(any())).thenAnswer(invocation -> {
      assertTrue(Thread.currentThread().isVirtual(),
          "the SDK call must run on a virtual thread, got " + Thread.currentThread());
      return posted();
    });

    StepVerifier.create(gateway.post(request()))
        .assertNext(result -> assertEquals(PostingResult.Status.POSTED, result.status()))
        .verifyComplete();
  }

  @Test
  void anInquiryRunsOnAVirtualThreadAndCarriesTheResult() {
    when(client.inquire("transfer-1")).thenAnswer(invocation -> {
      assertTrue(Thread.currentThread().isVirtual());
      return Optional.of(posted());
    });

    StepVerifier.create(gateway.inquire("transfer-1"))
        .assertNext(found -> assertEquals("CT1", found.orElseThrow().coreTxnId()))
        .verifyComplete();
  }

  @Test
  void anInquiryForAnUnknownTransferIsEmpty() {
    when(client.inquire("transfer-1")).thenReturn(Optional.empty());

    StepVerifier.create(gateway.inquire("transfer-1"))
        .assertNext(found -> assertTrue(found.isEmpty()))
        .verifyComplete();
  }

  /**
   * The core's ten sessions are the limit, whatever the caller: 200 concurrent callers never exceed
   * ten open sessions at once.
   */
  @Test
  void concurrencyNeverExceedsTheSessionLimit() throws Exception {
    int callers = 200;
    ConcurrencyProbe probe = new ConcurrencyProbe();
    CountDownLatch held = new CountDownLatch(1);
    when(client.post(any())).thenAnswer(invocation -> {
      probe.enter();
      try {
        held.await();
        return posted();
      } finally {
        probe.exit();
      }
    });

    for (int i = 0; i < callers; i++) {
      gateway.post(request()).subscribe();
    }
    // Let every caller reach the semaphore.
    awaitCondition(() -> probe.most() > 0);
    held.countDown();
    awaitCondition(() -> probe.inside() == 0);

    assertEquals(MAX_SESSIONS, probe.most(), "the core saw more than its session limit");
  }

  /** INC-102: a slow core occupies a session, not an event-loop thread. */
  @Test
  void aSlowCoreDoesNotBlockTheCallerThread() {
    CountDownLatch release = new CountDownLatch(1);
    when(client.post(any())).thenAnswer(invocation -> {
      release.await();
      return posted();
    });

    long startedAt = System.nanoTime();
    gateway.post(request()).subscribe();
    long returned = System.nanoTime() - startedAt;

    assertTrue(returned < 1_000_000_000L,
        "the subscriber thread waited " + returned + " ns; the blocking belongs to a virtual"
            + " thread");
    release.countDown();
  }

  /**
   * INC-101: a timeout means the posting may already exist. Retrying it is how money moves twice,
   * so the gateway propagates the failure instead of sending the request again.
   */
  @Test
  void aCoreTimeoutIsNeverRetried() {
    when(client.post(any())).thenThrow(new CoreTimeoutException("POST /core/postings"));

    StepVerifier.create(gateway.post(request()))
        .expectError(CoreTimeoutException.class)
        .verify();

    verify(client, times(1)).post(any());
  }

  @Test
  void aBusyCoreIsRetiedBecauseNothingWasSent() {
    when(client.post(any()))
        .thenThrow(new CoreBusyException("no session free"))
        .thenReturn(posted());

    StepVerifier.create(gateway.post(request()))
        .assertNext(result -> assertEquals(PostingResult.Status.POSTED, result.status()))
        .verifyComplete();

    verify(client, times(2)).post(any());
  }

  @Test
  void anUnreachableCoreIsRetriedBecauseNothingWasSent() {
    when(client.post(any()))
        .thenThrow(new CoreUnavailableException("connection refused", null))
        .thenReturn(posted());

    StepVerifier.create(gateway.post(request()))
        .assertNext(result -> assertEquals(PostingResult.Status.POSTED, result.status()))
        .verifyComplete();

    verify(client, times(2)).post(any());
  }

  @Test
  void retriesOfABusyCoreAreBounded() {
    when(client.post(any())).thenThrow(new CoreBusyException("no session free"));

    // The attempts run out; Reactor wraps the last failure, with the core's answer as its cause.
    StepVerifier.create(gateway.post(request()))
        .expectErrorSatisfies(error -> {
          assertTrue(reactor.core.Exceptions.isRetryExhausted(error),
              "retries should have been exhausted, got " + error);
          assertTrue(error.getCause() instanceof CoreBusyException);
        })
        .verify();

    verify(client, atLeast(2)).post(any());
  }

  /** A permit lost to an interruption or a timeout would eventually starve the core. */
  @Test
  void aPermitIsReleasedAfterATimeout() throws Exception {
    when(client.post(any())).thenThrow(new CoreTimeoutException("POST /core/postings"));

    for (int i = 0; i < MAX_SESSIONS + 5; i++) {
      StepVerifier.create(gateway.post(request()))
          .expectError(CoreTimeoutException.class)
          .verify();
    }
    // Every permit is back: a further call still acquires one.
    org.mockito.Mockito.doReturn(posted()).when(client).post(any());
    StepVerifier.create(gateway.post(request()))
        .assertNext(result -> assertEquals(PostingResult.Status.POSTED, result.status()))
        .verifyComplete();
  }

  @Test
  void aRejectedPostingIsNotRetried() {
    when(client.post(any())).thenAnswer(invocation ->
        new PostingResult("transfer-1", null, PostingResult.Status.REJECTED, "NO_FUNDS", null));

    StepVerifier.create(gateway.post(request()))
        .assertNext(result -> assertEquals(PostingResult.Status.REJECTED, result.status()))
        .verifyComplete();

    verify(client, times(1)).post(any());
  }

  private static void awaitCondition(java.util.function.BooleanSupplier condition) {
    long deadline = System.currentTimeMillis() + 10_000;
    while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
      Thread.onSpinWait();
    }
    assertTrue(condition.getAsBoolean(), "condition never became true");
  }
}
