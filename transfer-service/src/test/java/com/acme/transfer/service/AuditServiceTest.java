package com.acme.transfer.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.acme.transfer.repository.AuditEventRepository;
import com.acme.transfer.repository.TransferEntity;
import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/** The audit trail records the outcome of a transfer, with the reason a support engineer needs. */
class AuditServiceTest {

  @Test
  void recordsTheStatusAndTheReason() {
    AuditEventRepository repository = mock(AuditEventRepository.class);
    when(repository.append(eq("transfer-1"), eq("TRANSFER_COMPLETED"), eq(null), any()))
        .thenReturn(Mono.just(1));
    AuditService auditService = new AuditService(repository);

    TransferEntity transfer = new TransferEntity("transfer-1", "COMPLETED", null, "2000000001",
        "2000000002", new BigDecimal("10.00"), "USD", new BigDecimal("10.00"), "USD", null,
        "CT1", "lunch", Instant.now(), Instant.now());

    StepVerifier.create(auditService.recordTransfer(transfer))
        .expectNext(1)
        .verifyComplete();
  }

  @Test
  void aFailureRecordsTheReasonCode() {
    AuditEventRepository repository = mock(AuditEventRepository.class);
    when(repository.append(eq("transfer-1"), eq("TRANSFER_PENDING"), eq("CORE_TIMEOUT"), any()))
        .thenReturn(Mono.just(1));
    AuditService auditService = new AuditService(repository);

    TransferEntity transfer = new TransferEntity("transfer-1", "PENDING", "CORE_TIMEOUT",
        "2000000001", "2000000002", new BigDecimal("10.00"), "USD", null, null, null, null,
        "lunch", Instant.now(), Instant.now());

    StepVerifier.create(auditService.recordTransfer(transfer))
        .expectNext(1)
        .verifyComplete();
  }
}
