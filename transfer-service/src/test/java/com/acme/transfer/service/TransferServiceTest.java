package com.acme.transfer.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.acme.core.sdk.CoreTimeoutException;
import com.acme.core.sdk.PostingRequest;
import com.acme.core.sdk.PostingResult;
import com.acme.transfer.client.Account;
import com.acme.transfer.client.AccountClient;
import com.acme.transfer.core.CoreGateway;
import com.acme.transfer.dto.TransferRequest;
import com.acme.transfer.events.TransferEvents;
import com.acme.transfer.repository.IdempotencyEntity;
import com.acme.transfer.repository.TransferEntity;
import com.acme.transfer.repository.TransferRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Unit tests of the transfer pipeline: one happy path and the three outcomes the incidents are
 * about. Everything the pipeline talks to is stubbed with mocks, so these tests assert the
 * pipeline's decisions alone — the real HTTP, core and database behaviour is covered by the
 * integration tests.
 */
class TransferServiceTest {

  private static final String KEY = "key-12345678";
  private static final String SOURCE = "2000000001";
  private static final String DESTINATION = "2000000002";

  private AccountClient accountClient;
  private FxService fxService;
  private FraudService fraudService;
  private CoreGateway coreGateway;
  private TransferRepository transferRepository;
  private R2dbcEntityTemplate template;
  private AuditService auditService;
  private IdempotencyService idempotencyService;
  private TransferEvents events;
  private TransferService transferService;

  @BeforeEach
  void setUp() {
    accountClient = mock(AccountClient.class);
    fxService = mock(FxService.class);
    fraudService = mock(FraudService.class);
    coreGateway = mock(CoreGateway.class);
    transferRepository = mock(TransferRepository.class);
    template = mock(R2dbcEntityTemplate.class);
    auditService = mock(AuditService.class);
    idempotencyService = mock(IdempotencyService.class);
    events = new TransferEvents(new SimpleMeterRegistry());
    ObjectMapper objectMapper = new ObjectMapper();
    transferService = new TransferService(accountClient, fxService, fraudService, coreGateway,
        transferRepository, template, auditService, idempotencyService, events, objectMapper);

    when(accountClient.getAccount(SOURCE)).thenReturn(Mono.just(
        new Account(SOURCE, "Test", "USD", "ACTIVE", new BigDecimal("100000.00"))));
    when(accountClient.getAccount(DESTINATION)).thenReturn(Mono.just(
        new Account(DESTINATION, "Test", "USD", "ACTIVE", new BigDecimal("100000.00"))));
    when(fxService.convert(any(), anyString(), anyString()))
        .thenReturn(Mono.just(new Conversion(new BigDecimal("150.00"), "USD", null)));
    when(fraudService.check(anyString(), any(), any()))
        .thenReturn(Mono.just(FraudOutcome.ALLOW));
    when(template.insert(any(TransferEntity.class)))
        .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
    when(auditService.recordTransfer(any())).thenReturn(Mono.just(1));
    when(idempotencyService.begin(anyString(), anyString(), anyString()))
        .thenReturn(Mono.just(new IdempotencyService.Created()));
    when(idempotencyService.storeResponse(anyString(), anyInt(), any()))
        .thenReturn(Mono.just(1));
  }

  private static TransferRequest request() {
    return new TransferRequest(SOURCE, DESTINATION, "150.00", "USD", "test");
  }

  @Test
  void completedTransferPostsOnceAndStoresTheResponse() {
    when(coreGateway.post(any())).thenReturn(Mono.just(
        new PostingResult("ref", "CT1", PostingResult.Status.POSTED, null, Instant.now())));

    StepVerifier.create(transferService.execute(KEY, request()))
        .assertNext(result -> {
          assertEquals(201, result.httpStatus());
          assertNotNull(result.transfer());
          assertEquals("COMPLETED", result.transfer().status());
          assertNull(result.code());
        })
        .verifyComplete();

    verify(coreGateway, times(1)).post(any());
    verify(idempotencyService, times(1)).storeResponse(anyString(), anyInt(), any());
  }

  /**
   * INC-101 / INC-109: a core timeout means the posting may already exist, so the transfer becomes
   * PENDING with reason CORE_TIMEOUT and the core is never posted to a second time.
   */
  @Test
  void coreTimeoutBecomesPendingAndIsNeverReposted() {
    when(coreGateway.post(any()))
        .thenReturn(Mono.error(new CoreTimeoutException("core did not answer")));

    StepVerifier.create(transferService.execute(KEY, request()))
        .assertNext(result -> {
          assertEquals(202, result.httpStatus());
          assertNotNull(result.transfer());
          assertEquals("PENDING", result.transfer().status());
          assertEquals("CORE_TIMEOUT", result.transfer().reasonCode());
        })
        .verifyComplete();

    verify(coreGateway, times(1)).post(any());
  }

  /**
   * INC-104: a fraud service that cannot decide must not release the transfer. No core posting is
   * attempted at all.
   */
  @Test
  void fraudUnavailableHoldsTheTransferAndPostsNothing() {
    when(fraudService.check(anyString(), any(), any()))
        .thenReturn(Mono.just(FraudOutcome.UNAVAILABLE));

    StepVerifier.create(transferService.execute(KEY, request()))
        .assertNext(result -> {
          assertEquals(202, result.httpStatus());
          assertNotNull(result.transfer());
          assertEquals("PENDING_REVIEW", result.transfer().status());
          assertEquals("FRAUD_UNAVAILABLE", result.transfer().reasonCode());
        })
        .verifyComplete();

    verify(coreGateway, never()).post(any());
  }

  @Test
  void fraudDeniedRejectsTheTransferAndPostsNothing() {
    when(fraudService.check(anyString(), any(), any()))
        .thenReturn(Mono.just(FraudOutcome.DENY));

    StepVerifier.create(transferService.execute(KEY, request()))
        .assertNext(result -> {
          assertEquals(422, result.httpStatus());
          assertNotNull(result.transfer());
          assertEquals("REJECTED", result.transfer().status());
          assertEquals("FRAUD_DENIED", result.transfer().reasonCode());
        })
        .verifyComplete();

    verify(coreGateway, never()).post(any());
  }

  @Test
  void fraudReviewHoldsTheTransferAndPostsNothing() {
    when(fraudService.check(anyString(), any(), any()))
        .thenReturn(Mono.just(FraudOutcome.REVIEW));

    StepVerifier.create(transferService.execute(KEY, request()))
        .assertNext(result -> {
          assertEquals(202, result.httpStatus());
          assertEquals("PENDING_REVIEW", result.transfer().status());
          assertEquals("FRAUD_REVIEW", result.transfer().reasonCode());
        })
        .verifyComplete();

    verify(coreGateway, never()).post(any());
  }

  @Test
  void coreRejectingTheTransferIsRecordedAsRejected() {
    when(coreGateway.post(any())).thenReturn(Mono.just(
        new PostingResult("ref", null, PostingResult.Status.REJECTED, "NO_FUNDS", Instant.now())));

    StepVerifier.create(transferService.execute(KEY, request()))
        .assertNext(result -> {
          assertEquals(422, result.httpStatus());
          assertEquals("REJECTED", result.transfer().status());
          assertEquals("CORE_REJECTED", result.transfer().reasonCode());
        })
        .verifyComplete();
  }

  @Test
  void unknownSourceAccountRejectsBeforeAnythingIsPosted() {
    when(accountClient.getAccount(SOURCE)).thenReturn(Mono.empty());

    StepVerifier.create(transferService.execute(KEY, request()))
        .assertNext(result -> {
          assertEquals(422, result.httpStatus());
          assertEquals("REJECTED", result.transfer().status());
          assertEquals("ACCOUNT_NOT_FOUND", result.transfer().reasonCode());
        })
        .verifyComplete();

    verify(coreGateway, never()).post(any());
  }

  @Test
  void invalidRequestIsRejectedBeforeIdempotency() {
    TransferRequest bad = new TransferRequest(SOURCE, DESTINATION, "-1.00", "USD", "test");

    StepVerifier.create(transferService.execute(KEY, bad))
        .assertNext(result -> {
          assertEquals(400, result.httpStatus());
          assertEquals("VALIDATION_ERROR", result.code());
          assertNull(result.transfer());
        })
        .verifyComplete();

    verify(idempotencyService, never()).begin(anyString(), anyString(), anyString());
    verify(coreGateway, never()).post(any());
  }

  @Test
  void malformedIdempotencyKeyIsRejected() {
    StepVerifier.create(transferService.execute("short", request()))
        .assertNext(result -> {
          assertEquals(400, result.httpStatus());
          assertEquals("VALIDATION_ERROR", result.code());
        })
        .verifyComplete();

    verify(idempotencyService, never()).begin(anyString(), anyString(), anyString());
  }

  @Test
  void reusedKeyWithADifferentBodyIsRefused() {
    IdempotencyEntity existing = new IdempotencyEntity(KEY, "a-different-fingerprint",
        "other-transfer", 201, "{\"transferId\":\"other-transfer\"}", Instant.now());
    when(idempotencyService.begin(anyString(), anyString(), anyString()))
        .thenReturn(Mono.just(new IdempotencyService.Existing(existing)));

    StepVerifier.create(transferService.execute(KEY, request()))
        .assertNext(result -> {
          assertEquals(422, result.httpStatus());
          assertEquals("IDEMPOTENCY_KEY_REUSED", result.code());
        })
        .verifyComplete();

    verify(coreGateway, never()).post(any());
  }

  @Test
  void thePostingCarriesTheConvertedCreditAmount() {
    when(fxService.convert(any(), anyString(), anyString()))
        .thenReturn(Mono.just(
            new Conversion(new BigDecimal("150.00"), "USD", new BigDecimal("1.5"))));
    when(coreGateway.post(any())).thenReturn(Mono.just(
        new PostingResult("ref", "CT1", PostingResult.Status.POSTED, null, Instant.now())));

    StepVerifier.create(transferService.execute(KEY, request()))
        .assertNext(result -> {
          assertEquals(new BigDecimal("150.00"), new BigDecimal(result.transfer().credit().amount()));
          assertEquals("USD", result.transfer().credit().currency());
        })
        .verifyComplete();
  }
}
