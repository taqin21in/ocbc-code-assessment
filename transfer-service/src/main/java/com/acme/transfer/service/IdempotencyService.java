package com.acme.transfer.service;

import com.acme.transfer.dto.TransferRequest;
import com.acme.transfer.dto.TransferResource;
import com.acme.transfer.repository.IdempotencyEntity;
import com.acme.transfer.repository.IdempotencyRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

/**
 * Idempotency where the database is the synchronization boundary (INC-105).
 *
 * <h3>The problem that was here</h3>
 * <pre>
 * SELECT the key
 *   → missing → INSERT
 * </pre>
 * Two concurrent retries both observe "missing", both insert, and the customer pays twice. The
 * fingerprint was {@code request.hashCode()}, which also made it unstable across restarts.
 *
 * <h3>What this does instead</h3>
 *
 * {@link #begin(String, String, String)} inserts the key first, with the fingerprint and a
 * {@code null} response. The unique constraint from migration V3 makes exactly one insert win.
 * The loser sees the constraint violation and reads the winner's row:
 *
 * <ul>
 *   <li>same fingerprint → the same logical request: it waits for the winner's stored response
 *       and replays it, so every caller gets the same result and the core sees one posting;</li>
 *   <li>different fingerprint → {@code 422 IDEMPOTENCY_KEY_REUSED};</li>
 *   <li>no response yet when the patience runs out → {@code 409 REQUEST_IN_PROGRESS}.</li>
 * </ul>
 *
 * <p>After a restart, the row is still there: a key whose response never arrived still answers
 * 409, because the request may genuinely be in flight on a node we can no longer see.
 */
@Slf4j
@Service
public class IdempotencyService {

  /** How long a duplicate waits for the first request to finish before answering 409. */
  static final Duration MAX_WAIT = Duration.ofSeconds(15);

  private static final Duration POLL_INTERVAL = Duration.ofMillis(100);

  private final R2dbcEntityTemplate template;
  private final IdempotencyRepository idempotencyRepository;
  private final ObjectMapper objectMapper;

  public IdempotencyService(R2dbcEntityTemplate template, IdempotencyRepository idempotencyRepository,
                            ObjectMapper objectMapper) {
    this.template = template;
    this.idempotencyRepository = idempotencyRepository;
    this.objectMapper = objectMapper;
  }

  /**
   * Inserts the placeholder row for a key. Exactly one caller per key gets {@link Created}; every
   * concurrent caller gets the existing row.
   */
  public Mono<BeginOutcome> begin(String idempotencyKey, String fingerprint, String transferId) {
    IdempotencyEntity placeholder = new IdempotencyEntity(idempotencyKey, fingerprint, transferId,
        null, null, Instant.now());
    return template.insert(placeholder)
        .<BeginOutcome>map(row -> new Created())
        .onErrorResume(DuplicateKeyException.class, duplicate ->
            idempotencyRepository.findById(idempotencyKey)
                .map(existing -> (BeginOutcome) new Existing(existing))
                .switchIfEmpty(Mono.error(duplicate)));
  }

  /** Stores the response of the request that won the key, for the callers that follow. */
  public Mono<Integer> storeResponse(String idempotencyKey, int responseStatus,
                                     TransferResource response) {
    String body;
    try {
      body = objectMapper.writeValueAsString(response);
    } catch (JsonProcessingException e) {
      log.error("Could not serialize the response of key {}", idempotencyKey, e);
      return Mono.error(e);
    }
    return idempotencyRepository.storeResponse(idempotencyKey, response.transferId(), responseStatus,
        body);
  }

  /** Waits until the first request for the key has stored its response, then replays it. */
  public Mono<IdempotencyEntity> waitForResponse(String idempotencyKey) {
    return idempotencyRepository.findById(idempotencyKey)
        .switchIfEmpty(Mono.error(new NotReady()))
        .flatMap(row -> row.responseBody() == null
            ? Mono.<IdempotencyEntity>error(new NotReady())
            : Mono.just(row))
        .retryWhen(Retry.fixedDelay(MAX_WAIT.dividedBy(POLL_INTERVAL), POLL_INTERVAL)
            .filter(NotReady.class::isInstance)
            .onRetryExhaustedThrow((spec, signal) -> new RequestInProgressException(idempotencyKey)));
  }

  /** Internal marker: the row exists but its response is not stored yet. */
  private static final class NotReady extends RuntimeException {

    private NotReady() {
      super("response not stored yet", null, false, false);
    }
  }

  /** The stored response of a key, or empty when the first request is still running. */
  public TransferResource readResponse(IdempotencyEntity entity) {
    try {
      return objectMapper.readValue(entity.responseBody(), TransferResource.class);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Stored response is not readable", e);
    }
  }

  /** The database answered "first request still in progress" once too often. */
  public static final class RequestInProgressException extends RuntimeException {

    public RequestInProgressException(String idempotencyKey) {
      super("A request with Idempotency-Key " + idempotencyKey + " is still in progress.");
    }
  }

  /** The caller's key was taken: its placeholder row is now the owner of the key. */
  public sealed interface BeginOutcome permits Created, Existing {
  }

  /** The caller is the first for this key and owns the placeholder row it just inserted. */
  public record Created() implements BeginOutcome {
  }

  /** The key is taken; the row says by whom and whether the answer is known. */
  public record Existing(IdempotencyEntity row) implements BeginOutcome {
  }
}
