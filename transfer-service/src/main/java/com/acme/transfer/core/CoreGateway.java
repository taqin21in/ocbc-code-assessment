package com.acme.transfer.core;

import com.acme.core.sdk.PostingRequest;
import com.acme.core.sdk.PostingResult;
import java.util.Optional;
import reactor.core.publisher.Mono;

/**
 * The single reactive way in and out of the blocking core banking SDK.
 *
 * <p>Implementations run every SDK call on a virtual thread, share the core's session limit across
 * the whole service (transfer API, batch and reconciliation), and never retry a request whose
 * outcome is unknown.
 */
public interface CoreGateway {

  /**
   * Posts a transfer. <b>Not idempotent.</b> A {@link com.acme.core.sdk.CoreTimeoutException}
   * means the core may have applied the posting: it is propagated, never retried by this gateway.
   */
  Mono<PostingResult> post(PostingRequest request);

  /** Looks up a posting by reference. Read-only, safe to retry. */
  Mono<Optional<PostingResult>> inquire(String reference);
}
