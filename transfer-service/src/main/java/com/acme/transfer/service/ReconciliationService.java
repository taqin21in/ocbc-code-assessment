package com.acme.transfer.service;

import com.acme.core.sdk.PostingResult;
import com.acme.transfer.config.AcmeProperties;
import com.acme.transfer.core.CoreGateway;
import com.acme.transfer.events.TransferEvents;
import com.acme.transfer.repository.TransferEntity;
import com.acme.transfer.repository.TransferRepository;
import com.acme.transfer.web.ApiException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * FEAT-202: reconciliation of transfers the request path left open.
 *
 * <h3>What it looks at</h3>
 * Only transfers that are still {@code PENDING} — the ones a core timeout (or an unreadable core
 * answer) left behind. It never retries anything: the vendor job that was here re-POSTed timed-out
 * transfers from a fixed 50-thread pool (INC-108, INC-109), which is how the core saw the same
 * reference twice and customers were debited twice. This job only ever inquires: {@code inquire()}
 * is read-only, so a pass cannot move money, and a transfer that the core reports missing past its
 * grace period becomes {@code FAILED / NOT_POSTED} rather than something that posts again.
 *
 * <h3>Where it runs</h3>
 * The pass itself is plain imperative Java on virtual threads: blocking the reactive repository and
 * the blocking core SDK is correct there and nowhere else. It never touches the WebFlux event loop.
 * The core's ten sessions are the same ten the API and the batch endpoint use — the work shares
 * {@link CoreGateway}'s semaphore, so a reconciliation pass can never crowd out live transfers.
 *
 * <h3>One pass at a time</h3>
 * A scheduled pass and a manual one cannot overlap: the first taker of the flag runs, the second
 * gets {@code 409 RECONCILIATION_RUNNING}. One failing transfer never affects the others, and
 * every status change publishes an event.
 */
@Slf4j
@Service
public class ReconciliationService {

  private static final String MDC_KEY = "correlationId";

  private final TransferRepository transferRepository;
  private final CoreGateway coreGateway;
  private final TransferEvents events;
  private final AcmeProperties properties;
  private final ExecutorService virtualThreadExecutor;

  private final AtomicBoolean running = new AtomicBoolean();
  private final AtomicInteger passNumber = new AtomicInteger();

  public ReconciliationService(TransferRepository transferRepository, CoreGateway coreGateway,
                               TransferEvents events, AcmeProperties properties,
                               @Qualifier("virtualThreadExecutor")
                               ExecutorService virtualThreadExecutor) {
    this.transferRepository = transferRepository;
    this.coreGateway = coreGateway;
    this.events = events;
    this.properties = properties;
    this.virtualThreadExecutor = virtualThreadExecutor;
  }

  @Scheduled(fixedDelayString = "${acme.reconciliation.interval-seconds:30}000")
  public void scheduledRun() {
    startPass().subscribe(
        stats -> log.info("Scheduled reconciliation pass {} done in {} ms: {}", passNumber.get(),
            stats.durationMs(), stats),
        failure -> {
          if (isAlreadyRunning(failure)) {
            log.debug("The scheduled reconciliation pass found one already running");
          } else {
            log.warn("The scheduled reconciliation pass failed", failure);
          }
        },
        () -> {
        });
  }

  /** One pass, for the manual endpoint. Answers 409 when a pass is already running. */
  public Mono<ReconciliationStats> runOnce() {
    return startPass();
  }

  private Mono<ReconciliationStats> startPass() {
    if (!running.compareAndSet(false, true)) {
      return Mono.error(new ApiException(
          org.springframework.http.HttpStatus.CONFLICT,
          ApiException.Codes.RECONCILIATION_RUNNING,
          "A reconciliation pass is already running; wait for it to finish."));
    }
    int pass = passNumber.incrementAndGet();
    return Mono.fromCallable(() -> reconcile(pass))
        .subscribeOn(Schedulers.fromExecutorService(virtualThreadExecutor))
        .doFinally(signal -> running.set(false));
  }

  /** The whole pass, on one virtual thread; each transfer it finds gets its own. */
  private ReconciliationStats reconcile(int pass) {
    MDC.put(MDC_KEY, "reconciliation-" + pass);
    long startedAt = System.currentTimeMillis();
    try {
      List<TransferEntity> pending = transferRepository.findByStatus("PENDING")
          .collectList()
          .block();
      if (pending == null || pending.isEmpty()) {
        log.info("Reconciliation pass {}: no PENDING transfers", pass);
        return ReconciliationStats.empty(System.currentTimeMillis() - startedAt);
      }
      log.info("Reconciliation pass {}: {} PENDING transfers to inquire about", pass,
          pending.size());

      AtomicInteger completed = new AtomicInteger();
      AtomicInteger rejected = new AtomicInteger();
      AtomicInteger failed = new AtomicInteger();
      AtomicInteger stillPending = new AtomicInteger();

      // One virtual thread per transfer: the semaphore, not this executor, limits core work.
      CountDownLatch done = new CountDownLatch(pending.size());
      for (TransferEntity transfer : pending) {
        virtualThreadExecutor.execute(() -> {
          try {
            switch (reconcileOne(transfer)) {
              case COMPLETED -> completed.incrementAndGet();
              case REJECTED -> rejected.incrementAndGet();
              case FAILED -> failed.incrementAndGet();
              case STILL_PENDING -> stillPending.incrementAndGet();
            }
          } catch (RuntimeException failure) {
            log.warn("Reconciliation of transfer {} failed, leaving it PENDING",
                transfer.transferId(), failure);
            stillPending.incrementAndGet();
          } finally {
            done.countDown();
          }
        });
      }
      try {
        done.await();
      } catch (InterruptedException interrupted) {
        // Shutdown or cancellation: count what finished so far and stop this pass.
        log.warn("Reconciliation pass {} was interrupted", pass, interrupted);
        Thread.currentThread().interrupt();
      }
      Duration elapsed = Duration.ofMillis(System.currentTimeMillis() - startedAt);
      log.info("Reconciliation pass {} finished in {}: {}", pass, elapsed,
          new ReconciliationStats(pending.size(), completed.get(), rejected.get(),
              failed.get(), stillPending.get(), elapsed.toMillis()));
      return new ReconciliationStats(pending.size(), completed.get(), rejected.get(),
          failed.get(), stillPending.get(), elapsed.toMillis());
    } finally {
      MDC.remove(MDC_KEY);
    }
  }

  /**
   * One transfer through the decision matrix.
   *
   * <pre>
   * inquire → POSTED       → COMPLETED, and the core transaction id is stored
   * inquire → REJECTED     → REJECTED, CORE_REJECTED
   * inquire → not found, PENDING longer than the grace period → FAILED, NOT_POSTED
   * inquire → not found, younger, or the inquiry itself failed → stays PENDING
   * </pre>
   */
  ReconciliationOutcome reconcileOne(TransferEntity transfer) {
    Optional<PostingResult> found;
    try {
      found = coreGateway.inquire(transfer.transferId()).block();
    } catch (RuntimeException inquiryFailure) {
      // An inquiry is read-only, so a failed one changes nothing: the transfer stays PENDING and
      // the next pass tries again. It must never become FAILED or COMPLETED on a guess.
      log.warn("Inquiry for transfer {} failed, leaving it PENDING", transfer.transferId(),
          inquiryFailure);
      return ReconciliationOutcome.STILL_PENDING;
    }
    if (found.isEmpty()) {
      if (isPastGracePeriod(transfer)) {
        log.info("Transfer {} was never posted and is past the grace period: FAILED",
            transfer.transferId());
        apply(transfer, "FAILED", "NOT_POSTED", null);
        return ReconciliationOutcome.FAILED;
      }
      log.info("Transfer {} is still unknown to the core and inside the grace period",
          transfer.transferId());
      return ReconciliationOutcome.STILL_PENDING;
    }
    PostingResult posting = found.get();
    if (posting.status() == PostingResult.Status.POSTED) {
      log.info("Transfer {} is POSTED in core banking: COMPLETED", transfer.transferId());
      apply(transfer, "COMPLETED", null, posting.coreTxnId());
      return ReconciliationOutcome.COMPLETED;
    }
    log.info("Transfer {} was REJECTED by core banking: {}", transfer.transferId(),
        posting.reasonCode());
    apply(transfer, "REJECTED", "CORE_REJECTED", posting.coreTxnId());
    return ReconciliationOutcome.REJECTED;
  }

  private boolean isPastGracePeriod(TransferEntity transfer) {
    Instant cutoff = Instant.now().minusSeconds(properties.reconciliation().graceSeconds());
    Instant changed = transfer.updatedAt();
    return changed != null && changed.isBefore(cutoff);
  }

  private void apply(TransferEntity transfer, String status, String reasonCode, String coreTxnId) {
    transferRepository.updateStatus(transfer.transferId(), status, reasonCode, coreTxnId,
        Instant.now()).block();
    events.publish(transfer.transferId(), transfer.sourceAccount(), status, reasonCode,
        Instant.now());
  }

  private static boolean isAlreadyRunning(Throwable failure) {
    return failure instanceof ApiException api
        && ApiException.Codes.RECONCILIATION_RUNNING.equals(api.getCode());
  }
}
