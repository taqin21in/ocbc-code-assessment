package com.acme.transfer.service;

import com.acme.core.sdk.CoreBankingException;
import com.acme.core.sdk.CoreBusyException;
import com.acme.core.sdk.CoreTimeoutException;
import com.acme.core.sdk.CoreUnavailableException;
import com.acme.core.sdk.PostingRequest;
import com.acme.core.sdk.PostingResult;
import com.acme.transfer.client.Account;
import com.acme.transfer.client.AccountClient;
import com.acme.transfer.core.CoreGateway;
import com.acme.transfer.dto.TransferRequest;
import com.acme.transfer.web.ApiException;
import com.acme.transfer.events.TransferEvents;
import com.acme.transfer.repository.IdempotencyEntity;
import com.acme.transfer.repository.TransferEntity;
import com.acme.transfer.repository.TransferRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

/**
 * One transfer, from request to stored outcome.
 *
 * <h3>No blind retry (INC-101, INC-109)</h3>
 * The vendor pipeline had {@code .retry(3)} around the whole transfer, core post included. When the
 * core timed out after applying the posting, the retry posted again and the customer was debited
 * twice, and the outcome was reported FAILED although the money had moved. Now a timeout after a
 * core POST is final for the request path: the transfer is stored as {@code PENDING} with reason
 * code {@code CORE_TIMEOUT} — the API answers 202 — and only reconciliation, through
 * {@code inquire()}, resolves it. Safe retries (a busy core that never accepted anything) live in
 * {@link CoreGateway} alone.
 *
 * <h3>Reactive end to end</h3>
 * Every step is a {@link Mono}. Accounts are fetched in parallel with {@link Mono#zip}; the only
 * blocking call in the pipeline, the core SDK, happens behind {@link CoreGateway} on a virtual
 * thread. No event-loop thread blocks, anywhere.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TransferService {

  private final AccountClient accountClient;
  private final FxService fxService;
  private final FraudService fraudService;
  private final CoreGateway coreGateway;
  private final TransferRepository transferRepository;
  private final R2dbcEntityTemplate template;
  private final AuditService auditService;
  private final IdempotencyService idempotencyService;
  private final TransferEvents events;
  private final ObjectMapper objectMapper;

  /**
   * Runs one transfer behind an idempotency key: the single endpoint and every batch line come in
   * here, so there is one pipeline, not two.
   */
  public Mono<TransferResult> execute(String idempotencyKey, TransferRequest request) {
    String keyProblem = TransferRequests.validateIdempotencyKey(idempotencyKey);
    if (keyProblem != null) {
      return Mono.just(TransferResult.problem(400, ApiException.Codes.VALIDATION_ERROR, keyProblem));
    }
    String requestProblem = TransferRequests.validate(request);
    if (requestProblem != null) {
      return Mono.just(TransferResult.problem(400, ApiException.Codes.VALIDATION_ERROR,
          requestProblem));
    }

    String transferId = UUID.randomUUID().toString();
    String fingerprint = RequestFingerprint.sha256(request, objectMapper);
    return idempotencyService.begin(idempotencyKey, fingerprint, transferId)
        .flatMap(outcome -> switch (outcome) {
          case IdempotencyService.Created ignored ->
              createTransfer(idempotencyKey, transferId, request);
          case IdempotencyService.Existing existing ->
              replay(idempotencyKey, fingerprint, existing.row());
        })
        .onErrorResume(IdempotencyService.RequestInProgressException.class,
            inProgress -> Mono.just(TransferResult.problem(409,
                ApiException.Codes.REQUEST_IN_PROGRESS, inProgress.getMessage())));
  }

  public Mono<TransferEntity> getTransfer(String transferId) {
    return transferRepository.findById(transferId);
  }

  /** The key is taken: replay the stored answer, or say what is in the way. */
  private Mono<TransferResult> replay(String idempotencyKey, String fingerprint,
                                      IdempotencyEntity existing) {
    if (!existing.requestHash().equals(fingerprint)) {
      return Mono.just(TransferResult.problem(422, ApiException.Codes.IDEMPOTENCY_KEY_REUSED,
          "Idempotency-Key " + idempotencyKey + " was used with a different request body."));
    }
    if (existing.responseBody() != null) {
      return Mono.just(new TransferResult(
          existing.responseStatus() == null ? 500 : existing.responseStatus(),
          idempotencyService.readResponse(existing), null, null));
    }
    return idempotencyService.waitForResponse(idempotencyKey)
        .map(row -> new TransferResult(row.responseStatus(),
            idempotencyService.readResponse(row), null, null));
  }

  private Mono<TransferResult> createTransfer(String idempotencyKey, String transferId,
                                              TransferRequest request) {
    BigDecimal amount = new BigDecimal(request.amount());
    log.info("Creating transfer {}: {} {} from {} to {}", transferId, amount, request.currency(),
        request.sourceAccount(), request.destinationAccount());

    return accounts(request)
        .flatMap(pair -> evaluateAccounts(transferId, request, amount, pair.getT1(), pair.getT2()))
        .onErrorResume(CoreTimeoutException.class, timeout -> {
          // The posting may exist: PENDING, never FAILED, never posted again.
          log.warn("Core banking did not answer for transfer {}; leaving it PENDING for "
              + "reconciliation", transferId, timeout);
          return save(transferId, request, amount, null, "PENDING", "CORE_TIMEOUT", null);
        })
        .onErrorResume(CoreBankingException.class, unexpectedCore -> {
          // The core answered something the SDK cannot interpret: the outcome is just as unknown.
          log.warn("Core banking answered unexpectedly for transfer {}; leaving it PENDING",
              transferId, unexpectedCore);
          return save(transferId, request, amount, null, "PENDING", "CORE_ERROR", null);
        })
        .onErrorResume(failure -> dependencyUnavailable(transferId, request, amount, failure))
        .flatMap(entity -> storeResponseAndReturn(idempotencyKey, entity));
  }

  private Mono<TransferResult> storeResponseAndReturn(String idempotencyKey,
                                                     TransferEntity entity) {
    TransferResult result = TransferResult.of(entity);
    return idempotencyService.storeResponse(idempotencyKey, result.httpStatus(), result.transfer())
        .thenReturn(result);
  }

  /** A dependency that did not answer: no core posting was made or accepted, so FAILED is safe. */
  private Mono<TransferEntity> dependencyUnavailable(String transferId, TransferRequest request,
                                                     BigDecimal amount, Throwable failure) {
    if (!isDependencyFailure(failure)) {
      return Mono.error(failure);
    }
    log.warn("Transfer {} is FAILED (DEPENDENCY_UNAVAILABLE); nothing was posted", transferId,
        failure);
    return save(transferId, request, amount, null, "FAILED", "DEPENDENCY_UNAVAILABLE", null);
  }

  private boolean isDependencyFailure(Throwable failure) {
    return failure instanceof TimeoutException
        || failure instanceof ExpiredRateException
        || failure instanceof CoreBusyException
        || failure instanceof CoreUnavailableException
        || failure instanceof WebClientResponseException
        || (failure.getCause() != null && isDependencyFailure(failure.getCause()));
  }

  private Mono<Tuple2<Optional<Account>, Optional<Account>>> accounts(TransferRequest request) {
    return Mono.zip(
        accountClient.getAccount(request.sourceAccount())
            .map(Optional::<Account>of).defaultIfEmpty(Optional.empty()),
        accountClient.getAccount(request.destinationAccount())
            .map(Optional::<Account>of).defaultIfEmpty(Optional.empty()));
  }

  private Mono<TransferEntity> evaluateAccounts(String transferId, TransferRequest request,
                                                BigDecimal amount, Optional<Account> source,
                                                Optional<Account> destination) {
    if (source.isEmpty() || destination.isEmpty()) {
      return save(transferId, request, amount, null, "REJECTED", "ACCOUNT_NOT_FOUND", null);
    }
    Account src = source.get();
    Account dst = destination.get();
    if (!"ACTIVE".equals(src.status()) || !"ACTIVE".equals(dst.status())) {
      return save(transferId, request, amount, null, "REJECTED", "ACCOUNT_NOT_ACTIVE", null);
    }
    if (!src.currency().equals(request.currency())) {
      return save(transferId, request, amount, null, "REJECTED", "CURRENCY_MISMATCH", null);
    }
    if (src.available().compareTo(amount) < 0) {
      return save(transferId, request, amount, null, "REJECTED", "INSUFFICIENT_FUNDS", null);
    }
    return convertAndPost(transferId, request, amount, src, dst);
  }

  private Mono<TransferEntity> convertAndPost(String transferId, TransferRequest request,
                                              BigDecimal amount, Account source,
                                              Account destination) {
    return fxService.convert(amount, source.currency(), destination.currency())
        .flatMap(conversion -> fraudService.check(transferId, request, amount)
            .flatMap(outcome -> switch (outcome) {
              case DENY -> save(transferId, request, amount, conversion, "REJECTED",
                  "FRAUD_DENIED", null);
              case REVIEW -> save(transferId, request, amount, conversion, "PENDING_REVIEW",
                  "FRAUD_REVIEW", null);
              // INC-104: no money moves while fraud cannot decide.
              case UNAVAILABLE -> save(transferId, request, amount, conversion, "PENDING_REVIEW",
                  "FRAUD_UNAVAILABLE", null);
              case ALLOW -> post(transferId, request, amount, conversion, source, destination);
            }));
  }

  private Mono<TransferEntity> post(String transferId, TransferRequest request, BigDecimal amount,
                                    Conversion conversion, Account source, Account destination) {
    PostingRequest posting = new PostingRequest(transferId, source.accountNo(),
        destination.accountNo(), amount, request.currency(), conversion.creditAmount(),
        conversion.creditCurrency());
    log.info("Posting transfer {} through core banking", transferId);
    return coreGateway.post(posting)
        .flatMap(result -> result.status() == PostingResult.Status.POSTED
            ? save(transferId, request, amount, conversion, "COMPLETED", null, result.coreTxnId())
            : save(transferId, request, amount, conversion, "REJECTED", "CORE_REJECTED", null));
  }

  /** Stores one outcome, publishes its event and records the audit trail. */
  Mono<TransferEntity> save(String transferId, TransferRequest request, BigDecimal amount,
                            Conversion conversion, String status, String reasonCode,
                            String coreTxnId) {
    Instant now = Instant.now();
    TransferEntity transfer = new TransferEntity(transferId, status, reasonCode,
        request.sourceAccount(), request.destinationAccount(), amount, request.currency(),
        conversion == null ? null : conversion.creditAmount(),
        conversion == null ? null : conversion.creditCurrency(),
        conversion == null ? null : conversion.rate(),
        coreTxnId, request.description(), now, now);
    log.info("Transfer {} is {} {}", transferId, status, reasonCode == null ? "" : reasonCode);
    return template.insert(transfer)
        .doOnNext(entity -> {
          events.publish(transferId, request.sourceAccount(), entity.status(), entity.reasonCode(),
              entity.updatedAt());
          auditService.recordTransfer(entity).subscribe(
              ok -> {
              },
              error -> log.error("Could not write the audit event for transfer {}", transferId,
                  error));
        });
  }
}
