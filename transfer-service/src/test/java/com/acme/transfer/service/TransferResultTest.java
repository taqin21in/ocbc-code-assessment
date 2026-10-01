package com.acme.transfer.service;

import com.acme.transfer.dto.TransferResource;
import com.acme.transfer.repository.TransferEntity;
import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The status the API contract assigns to each outcome, and the mapping from the stored entity to
 * the resource on the wire.
 */
class TransferResultTest {

  @Test
  void completedTransfersAreCreated() {
    assertEquals(201, TransferResult.httpStatus("COMPLETED"));
  }

  @Test
  void pendingTransfersAreAccepted() {
    assertEquals(202, TransferResult.httpStatus("PENDING"));
    assertEquals(202, TransferResult.httpStatus("PENDING_REVIEW"));
  }

  @Test
  void rejectedTransfersAreUnprocessable() {
    assertEquals(422, TransferResult.httpStatus("REJECTED"));
  }

  @Test
  void anyOtherStatusIsAnInternalError() {
    assertEquals(503, TransferResult.httpStatus("FAILED"));
    assertEquals(503, TransferResult.httpStatus("UNKNOWN"));
  }

  @Test
  void anEntityBecomesTheResourceTheApiReturns() {
    Instant now = Instant.now();
    TransferEntity entity = new TransferEntity("transfer-1", "COMPLETED", null, "2000000001",
        "2000000002", new BigDecimal("10.00"), "USD", new BigDecimal("150.00"), "IDR",
        new BigDecimal("15000.00"), "CT1", "lunch", now, now);

    TransferResource resource = TransferResource.from(entity);

    assertEquals("transfer-1", resource.transferId());
    assertEquals("COMPLETED", resource.status());
    assertEquals("10.00", resource.debit().amount());
    assertEquals("USD", resource.debit().currency());
    assertEquals("150.00", resource.credit().amount());
    assertEquals("IDR", resource.credit().currency());
    assertEquals("15000", resource.fxRate());
    assertEquals("CT1", resource.coreTxnId());
  }

  @Test
  void anUnconvertedTransferHasNoCreditOrRate() {
    Instant now = Instant.now();
    TransferEntity entity = new TransferEntity("transfer-1", "REJECTED", "FRAUD_DENIED",
        "2000000001", "2000000002", new BigDecimal("10.00"), "USD", null, null, null, null,
        "lunch", now, now);

    TransferResource resource = TransferResource.from(entity);

    assertEquals("REJECTED", resource.status());
    assertEquals("FRAUD_DENIED", resource.reasonCode());
    assertEquals(null, resource.credit());
    assertEquals(null, resource.fxRate());
    assertEquals(null, resource.coreTxnId());
  }
}
