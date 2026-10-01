package com.acme.transfer.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.acme.transfer.dto.TransferResource;
import com.acme.transfer.repository.IdempotencyEntity;
import com.acme.transfer.repository.IdempotencyRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * INC-105: idempotency where the unique constraint, not a SELECT-then-INSERT, decides who owns a
 * key. Exactly one request runs; the rest replay its answer or are told the key was reused.
 */
class IdempotencyServiceTest {

  private static final String KEY = "key-12345678";

  private R2dbcEntityTemplate template;
  private IdempotencyRepository repository;
  private IdempotencyService service;

  @BeforeEach
  void setUp() {
    template = mock(R2dbcEntityTemplate.class);
    repository = mock(IdempotencyRepository.class);
    // As Spring configures it in production: Java 8 date/time types are readable.
    service = new IdempotencyService(template, repository,
        new ObjectMapper().registerModule(new JavaTimeModule()));
  }

  private static IdempotencyEntity placeholder() {
    return new IdempotencyEntity(KEY, "fingerprint", "transfer-1", null, null, Instant.now());
  }

  /** The row as it looks once the first request for the key has stored its answer. */
  private static IdempotencyEntity storedResponse() {
    return new IdempotencyEntity(KEY, "fingerprint", "transfer-1", 201, STORED_RESPONSE_BODY,
        Instant.now());
  }

  private static final String STORED_RESPONSE_BODY =
      "{\"transferId\":\"transfer-1\",\"status\":\"COMPLETED\",\"reasonCode\":null,"
          + "\"sourceAccount\":\"2000000001\",\"destinationAccount\":\"2000000002\","
          + "\"debit\":null,\"credit\":null,\"fxRate\":null,\"coreTxnId\":\"CT1\","
          + "\"createdAt\":\"2024-01-01T00:00:00Z\",\"updatedAt\":\"2024-01-01T00:00:00Z\"}";

  @Test
  void theFirstInsertWinsTheKey() {
    when(template.insert(any(IdempotencyEntity.class)))
        .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

    StepVerifier.create(service.begin(KEY, "fingerprint", "transfer-1"))
        .assertNext(outcome -> assertEquals(outcome.getClass(), IdempotencyService.Created.class))
        .verifyComplete();
  }

  /** The loser of the race sees the constraint violation and reads the winner's row instead. */
  @Test
  void aConcurrentDuplicateFindsTheExistingRow() {
    when(template.insert(any(IdempotencyEntity.class)))
        .thenReturn(Mono.error(new DuplicateKeyException("duplicate key")));
    when(repository.findById(KEY)).thenReturn(Mono.just(placeholder()));

    StepVerifier.create(service.begin(KEY, "fingerprint", "transfer-1"))
        .assertNext(outcome -> {
          assertEquals(outcome.getClass(), IdempotencyService.Existing.class);
          assertEquals("transfer-1",
              ((IdempotencyService.Existing) outcome).row().transferId());
        })
        .verifyComplete();
  }

  @Test
  void aDuplicateKeyThatVanishedIsStillAnError() {
    when(template.insert(any(IdempotencyEntity.class)))
        .thenReturn(Mono.error(new DuplicateKeyException("duplicate key")));
    when(repository.findById(KEY)).thenReturn(Mono.empty());

    StepVerifier.create(service.begin(KEY, "fingerprint", "transfer-1"))
        .expectError(DuplicateKeyException.class)
        .verify();
  }

  @Test
  void theResponseIsStoredForTheCallersThatFollow() {
    TransferResource resource = new TransferResource("transfer-1", "COMPLETED", null,
        "2000000001", "2000000002", null, null, null, "CT1", Instant.now(), Instant.now());
    when(repository.storeResponse(eq(KEY), eq("transfer-1"), eq(201), anyString()))
        .thenReturn(Mono.just(1));

    StepVerifier.create(service.storeResponse(KEY, 201, resource))
        .expectNext(1)
        .verifyComplete();

    verify(repository).storeResponse(eq(KEY), eq("transfer-1"), eq(201), anyString());
  }

  @Test
  void anUnserializableResponseFailsRatherThanBeingSilentlyLost() {
    TransferResource broken = mock(TransferResource.class);
    when(broken.transferId()).thenThrow(new RuntimeException("broken"));

    // Jackson wraps the broken accessor in a JsonMappingException; the service does not swallow it.
    StepVerifier.create(service.storeResponse(KEY, 201, broken))
        .expectError(JsonProcessingException.class)
        .verify();

    verify(repository, never()).storeResponse(anyString(), anyString(),
        anyInt(), anyString());
  }

  @Test
  void aStoredResponseIsReplayedToEveryCaller() {
    IdempotencyEntity stored = storedResponse();

    TransferResource replayed = service.readResponse(stored);

    assertEquals("transfer-1", replayed.transferId());
    assertEquals("COMPLETED", replayed.status());
    assertEquals("CT1", replayed.coreTxnId());
  }

  @Test
  void aCorruptedStoredResponseIsReportedAsABug() {
    IdempotencyEntity stored = new IdempotencyEntity(KEY, "fingerprint", "transfer-1", 201,
        "not json", Instant.now());

    org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
        () -> service.readResponse(stored));
  }

  @Test
  void aCallerWhoseResponseNeverArrivesEventuallyHearsThatTheRequestIsInProgress() {
    when(repository.findById(KEY)).thenReturn(Mono.just(placeholder()));

    StepVerifier.withVirtualTime(() -> service.waitForResponse(KEY))
        .expectSubscription()
        .thenAwait(java.time.Duration.ofSeconds(30))
        .expectError(IdempotencyService.RequestInProgressException.class)
        .verify();
  }

  @Test
  void aCallerWaitsUntilTheResponseArrives() {
    // A retry resubscribes to the same chain, which re-queries the repository: a fixed
    // Mono.just(...) stub would replay the placeholder forever, so the answer has to be
    // produced per subscription, as a real R2DBC query is.
    AtomicBoolean firstAttempt = new AtomicBoolean(true);
    when(repository.findById(KEY)).thenReturn(Mono.defer(() ->
        firstAttempt.getAndSet(false) ? Mono.just(placeholder()) : Mono.just(storedResponse())));

    StepVerifier.withVirtualTime(() -> service.waitForResponse(KEY))
        .expectSubscription()
        .thenAwait(java.time.Duration.ofMillis(150))
        .assertNext(row -> {
          assertNotNull(row.responseBody());
          assertEquals(201, row.responseStatus());
        })
        .verifyComplete();
  }

  @Test
  void aKeyThatNoLongerExistsIsNotAwaited() {
    when(repository.findById(KEY)).thenReturn(Mono.empty());

    StepVerifier.create(service.waitForResponse(KEY))
        .expectError(RuntimeException.class)
        .verify();
  }
}
