package com.acme.transfer.core;

import com.acme.core.sdk.CoreBankingClient;
import com.acme.core.sdk.CoreBusyException;
import com.acme.core.sdk.CoreTimeoutException;
import com.acme.core.sdk.CoreUnavailableException;
import com.acme.core.sdk.PostingRequest;
import com.acme.core.sdk.PostingResult;
import com.acme.transfer.config.AcmeProperties;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

/**
 * The one place the blocking core banking SDK is called.
 *
 * <h3>Threading</h3>
 * Every SDK call runs inside {@code Mono.fromCallable(...).subscribeOn(virtualThreadScheduler)},
 * so no Netty event-loop thread is ever blocked: the SDK's socket reads happen on a virtual
 * thread, and the result flows back into the Reactor pipeline.
 *
 * <h3>Currency of sessions</h3>
 * The core allows at most 10 open sessions, for every caller together. One fair semaphore,
 * created once here, is shared by the transfer API, the batch endpoint and the reconciliation
 * worker. A permit is acquired before the SDK call and released in a {@code finally}, so a permit
 * is never lost, not even on timeout. When 200 requests arrive at once, the core sees 10 open
 * sessions and 190 callers queued on the semaphore — the core, not our thread budget, is the
 * limit.
 *
 * <h3>Retries</h3>
 * {@link CoreBusyException} (no session was free) and {@link CoreUnavailableException} (the core
 * could not be reached) mean nothing was sent: those are retried with backoff. Everything else —
 * in particular {@link CoreTimeoutException}, which means the request <em>was</em> sent and the
 * outcome is unknown — is propagated untouched. Re-posting on a timeout is how money moves twice.
 */
@Slf4j
@Component
public class DefaultCoreGateway implements CoreGateway {

  private final CoreBankingClient client;
  private final AcmeProperties properties;
  private final ExecutorService virtualThreadExecutor;
  private final Semaphore sessions;

  public DefaultCoreGateway(CoreBankingClient client, AcmeProperties properties,
                            @Qualifier("virtualThreadExecutor")
                            ExecutorService virtualThreadExecutor) {
    this.client = client;
    this.properties = properties;
    this.virtualThreadExecutor = virtualThreadExecutor;
    this.sessions = new Semaphore(properties.coreMaxSessions(), true);
  }

  @Override
  public Mono<PostingResult> post(PostingRequest request) {
    return withPermit(() -> client.post(request))
        .retryWhen(retryWhenNothingWasSent());
  }

  @Override
  public Mono<Optional<PostingResult>> inquire(String reference) {
    return withPermit(() -> client.inquire(reference))
        .retryWhen(retryWhenNothingWasSent());
  }

  /** Only retries failures the SDK documents as "nothing was sent". */
  private Retry retryWhenNothingWasSent() {
    return Retry.backoff(properties.coreBusyRetryAttempts(), properties.coreBusyMinBackoff())
        .filter(e -> e instanceof CoreBusyException || e instanceof CoreUnavailableException)
        .doBeforeRetry(signal -> log.debug("Core call failed with {} (nothing sent), retrying",
            signal.failure().getClass().getName()));
  }

  /**
   * Runs one blocking SDK call on a virtual thread, holding one of the core's session permits
   * around it. {@code acquireUninterruptibly} keeps the permit accounting correct when the
   * carrier thread is interrupted; {@code release} in {@code finally} keeps it correct when the
   * call times out.
   */
  private <T> Mono<T> withPermit(Supplier<T> call) {
    return Mono.fromCallable(() -> {
      sessions.acquireUninterruptibly();
      try {
        return call.get();
      } finally {
        sessions.release();
      }
    }).subscribeOn(Schedulers.fromExecutorService(virtualThreadExecutor));
  }
}
