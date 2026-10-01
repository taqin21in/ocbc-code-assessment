package com.acme.transfer.repository;

import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Mono;

public interface IdempotencyRepository extends ReactiveCrudRepository<IdempotencyEntity, String> {

  /**
   * Records the outcome of the request that first took this key. Called once, when the transfer
   * pipeline has its answer; until then the row is the "request in progress" marker that concurrent
   * duplicates wait on.
   */
  @Modifying
  @Query("""
      UPDATE idempotency_keys
         SET transfer_id = :transferId,
             response_status = :responseStatus,
             response_body = :responseBody
       WHERE idempotency_key = :idempotencyKey
      """)
  Mono<Integer> storeResponse(String idempotencyKey, String transferId, Integer responseStatus,
                              String responseBody);
}
