package com.acme.transfer.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.acme.core.sdk.PostingResult;
import com.acme.transfer.config.AcmeProperties;
import com.acme.transfer.core.CoreGateway;
import com.acme.transfer.events.TransferEvents;
import com.acme.transfer.repository.TransferEntity;
import com.acme.transfer.repository.TransferRepository;
import com.acme.transfer.web.ApiException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * FEAT-202: reconciliation never posts, only inquires. The decision matrix is tested here one
 * branch at a time, so a change to any outcome is visible.
 */
class ReconciliationServiceTest {

  private static final int GRACE_SECONDS = 10;

  private TransferRepository repository;
  private CoreGateway coreGateway;
  private TransferEvents events;
  private ReconciliationService service;
  private ExecutorService executor;

  @BeforeEach
  void setUp() {
    repository = mock(TransferRepository.class);
    coreGateway = mock(CoreGateway.class);
    // The pass writes its decisions straight back through the repository.
    when(repository.updateStatus(anyString(), anyString(), any(), any(), any(Instant.class)))
        .thenReturn(Mono.empty());
    events = new TransferEvents(new SimpleMeterRegistry());
    executor = Executors.newVirtualThreadPerTaskExecutor();
    AcmeProperties properties = new AcmeProperties(
        "http://accounts", "http://fx", "http://fraud", "http://core",
        2000, 10, 3, Duration.ofMillis(1), Duration.ofSeconds(5), Duration.ofSeconds(5),
        Duration.ofMillis(1500),
        new AcmeProperties.Http(4, Duration.ofSeconds(1), Duration.ofSeconds(30), Duration.ofMillis(100)),
        new AcmeProperties.Reconciliation(30, GRACE_SECONDS),
        new AcmeProperties.FraudThreshold(new BigDecimal("75000000.00"),
            new BigDecimal("5000.00"), new BigDecimal("6500.00")),
        new AcmeProperties.Batch(1000, 8),
        new AcmeProperties.Sse(15));
    service = new ReconciliationService(repository, coreGateway, events, properties, executor);
  }

  @AfterEach
  void tearDown() {
    executor.shutdownNow();
  }

  private static TransferEntity pending() {
    return new TransferEntity("transfer-1", "PENDING", "CORE_TIMEOUT", "2000000001",
        "2000000002", new BigDecimal("10.00"), "USD", new BigDecimal("10.00"), "USD", null,
        null, "lunch", Instant.now(), Instant.now());
  }

  private static TransferEntity pendingSince(Instant updatedAt) {
    return new TransferEntity("transfer-1", "PENDING", "CORE_TIMEOUT", "2000000001",
        "2000000002", new BigDecimal("10.00"), "USD", new BigDecimal("10.00"), "USD", null,
        null, "lunch", updatedAt, updatedAt);
  }

  private static PostingResult posted() {
    return new PostingResult("transfer-1", "CT1", PostingResult.Status.POSTED, null, null);
  }

  private static PostingResult rejected() {
    return new PostingResult("transfer-1", "CT1", PostingResult.Status.REJECTED, "NO_FUNDS", null);
  }

  @Test
  void aPostedTransferBecomesCompletedWithTheCoreTransactionId() {
    when(coreGateway.inquire("transfer-1")).thenReturn(Mono.just(Optional.of(posted())));

    assertEquals(ReconciliationOutcome.COMPLETED, service.reconcileOne(pending()));

    verify(repository).updateStatus(eq("transfer-1"), eq("COMPLETED"), isNull(), eq("CT1"),
        any(Instant.class));
  }

  @Test
  void aRejectedTransferBecomesRejected() {
    when(coreGateway.inquire("transfer-1")).thenReturn(Mono.just(Optional.of(rejected())));

    assertEquals(ReconciliationOutcome.REJECTED, service.reconcileOne(pending()));

    verify(repository).updateStatus(eq("transfer-1"), eq("REJECTED"), eq("CORE_REJECTED"),
        eq("CT1"), any(Instant.class));
  }

  @Test
  void aMissingTransferPastTheGracePeriodBecomesFailed() {
    when(coreGateway.inquire("transfer-1")).thenReturn(Mono.just(Optional.empty()));

    TransferEntity old = pendingSince(Instant.now().minus(GRACE_SECONDS + 5, ChronoUnit.SECONDS));
    assertEquals(ReconciliationOutcome.FAILED, service.reconcileOne(old));

    verify(repository).updateStatus(eq("transfer-1"), eq("FAILED"), eq("NOT_POSTED"), isNull(),
        any(Instant.class));
  }

  @Test
  void aMissingTransferInsideTheGracePeriodStaysPending() {
    when(coreGateway.inquire("transfer-1")).thenReturn(Mono.just(Optional.empty()));

    TransferEntity recent = pendingSince(Instant.now());
    assertEquals(ReconciliationOutcome.STILL_PENDING, service.reconcileOne(recent));

    verify(repository, never()).updateStatus(anyString(), anyString(), anyString(), anyString(),
        any(Instant.class));
  }

  @Test
  void anInquiryThatFailsLeavesTheTransferPending() {
    when(coreGateway.inquire("transfer-1")).thenReturn(Mono.error(new IllegalStateException(
        "core down")));

    assertEquals(ReconciliationOutcome.STILL_PENDING, service.reconcileOne(pending()));

    verify(repository, never()).updateStatus(anyString(), anyString(), anyString(), anyString(),
        any(Instant.class));
  }

  @Test
  void anInquiryForATransferWithNoUpdatedAtStaysPending() {
    when(coreGateway.inquire("transfer-1")).thenReturn(Mono.just(Optional.empty()));

    TransferEntity noTimestamp = new TransferEntity("transfer-1", "PENDING", "CORE_TIMEOUT",
        "2000000001", "2000000002", new BigDecimal("10.00"), "USD", new BigDecimal("10.00"),
        "USD", null, null, "lunch", Instant.now(), null);

    assertEquals(ReconciliationOutcome.STILL_PENDING, service.reconcileOne(noTimestamp));
  }

  @Test
  void everyStatusChangePublishesAnEvent() throws Exception {
    when(coreGateway.inquire("transfer-1")).thenReturn(Mono.just(Optional.of(posted())));

    java.util.concurrent.CountDownLatch received = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.atomic.AtomicReference<String> status = new java.util.concurrent.atomic.AtomicReference<>();
    java.util.concurrent.atomic.AtomicReference<String> transferId = new java.util.concurrent.atomic.AtomicReference<>();
    events.stream().take(1).subscribe(event -> {
      status.set(event.status());
      transferId.set(event.transferId());
      received.countDown();
    });

    service.reconcileOne(pending());
    received.await(5, java.util.concurrent.TimeUnit.SECONDS);

    assertEquals("COMPLETED", status.get());
    assertEquals("transfer-1", transferId.get());
  }

  @Test
  void aPassWithNothingPendingReportsEmptyStats() {
    when(repository.findByStatus("PENDING")).thenReturn(Flux.empty());

    StepVerifier.create(service.runOnce())
        .assertNext(stats -> {
          assertEquals(0, stats.checked());
          assertEquals(0, stats.completed());
          assertEquals(0, stats.failed());
        })
        .verifyComplete();
  }

  @Test
  void aPassReconcilesEveryPendingTransfer() {
    TransferEntity second = new TransferEntity("transfer-2", "PENDING", "CORE_TIMEOUT",
        "2000000003", "2000000004", new BigDecimal("10.00"), "USD", new BigDecimal("10.00"),
        "USD", null, null, "lunch", Instant.now(), Instant.now());
    when(repository.findByStatus("PENDING")).thenReturn(Flux.just(pending(), second));
    when(coreGateway.inquire("transfer-1")).thenReturn(Mono.just(Optional.of(posted())));
    when(coreGateway.inquire("transfer-2")).thenReturn(Mono.just(Optional.of(rejected())));

    StepVerifier.create(service.runOnce())
        .assertNext(stats -> {
          assertEquals(2, stats.checked());
          assertEquals(1, stats.completed());
          assertEquals(1, stats.rejected());
          assertEquals(0, stats.stillPending());
        })
        .verifyComplete();
  }

  /** Two passes cannot overlap: the second answers 409 instead of doubling the load on the core. */
  @Test
  void aSecondPassWhileOneIsRunningIsRefused() throws Exception {
    java.util.concurrent.CountDownLatch started = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.CountDownLatch hold = new java.util.concurrent.CountDownLatch(1);
    when(repository.findByStatus("PENDING")).thenReturn(Flux.defer(() -> {
      started.countDown();
      com.acme.transfer.kit.VirtualThreads.await(hold);
      return Flux.<TransferEntity>empty();
    }));
    when(coreGateway.inquire(anyString())).thenReturn(Mono.just(Optional.of(posted())));

    service.runOnce().subscribe();
    started.await();

    StepVerifier.create(service.runOnce())
        .expectErrorSatisfies(error -> {
          ApiException api = (ApiException) error;
          assertEquals(409, api.getStatusCode());
          assertEquals(ApiException.Codes.RECONCILIATION_RUNNING, api.getCode());
        })
        .verify();

    hold.countDown();
  }

  @Test
  void aFailingTransferDoesNotStopThePass() {
    TransferEntity second = new TransferEntity("transfer-2", "PENDING", "CORE_TIMEOUT",
        "2000000003", "2000000004", new BigDecimal("10.00"), "USD", new BigDecimal("10.00"),
        "USD", null, null, "lunch", Instant.now(), Instant.now());
    when(repository.findByStatus("PENDING")).thenReturn(Flux.just(pending(), second));
    when(coreGateway.inquire("transfer-1")).thenReturn(Mono.just(Optional.of(posted())));
    when(coreGateway.inquire("transfer-2"))
        .thenReturn(Mono.error(new IllegalStateException("core down")));

    StepVerifier.create(service.runOnce())
        .assertNext(stats -> {
          assertEquals(2, stats.checked());
          assertEquals(1, stats.completed());
          assertEquals(1, stats.stillPending());
        })
        .verifyComplete();
  }

  @Test
  void thePassPublishesEventsForTheTransfersItMoved() {
    when(repository.findByStatus("PENDING")).thenReturn(Flux.just(pending()));
    when(coreGateway.inquire("transfer-1")).thenReturn(Mono.just(Optional.of(posted())));

    StepVerifier.create(service.runOnce()).expectNextCount(1).verifyComplete();

    verify(coreGateway, atLeastOnce()).inquire("transfer-1");
    verify(coreGateway, never()).post(any());
  }
}
