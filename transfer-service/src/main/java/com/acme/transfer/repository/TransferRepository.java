package com.acme.transfer.repository;

import java.time.Instant;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface TransferRepository extends ReactiveCrudRepository<TransferEntity, String> {

  /** The transfers reconciliation looks at: every status that the core may still resolve. */
  Flux<TransferEntity> findByStatus(String status);

  @Modifying
  @Query("""
      UPDATE transfers
         SET status = :status,
             reason_code = :reasonCode,
             core_txn_id = :coreTxnId,
             updated_at = :updatedAt
       WHERE transfer_id = :transferId
      """)
  Mono<Integer> updateStatus(String transferId, String status, String reasonCode, String coreTxnId,
                             Instant updatedAt);
}
