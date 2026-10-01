package com.acme.transfer.dto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

/** The batch protocol's two additions to a transfer request: {@code lineId} and the key. */
class BatchLineTest {

  @Test
  void aLineBecomesTheTransferRequestItCarries() {
    BatchLine line = new BatchLine("line-1", "key-12345678", "2000000001", "2000000002",
        "150.00", "USD", "lunch");

    TransferRequest request = line.toRequest();

    assertEquals("2000000001", request.sourceAccount());
    assertEquals("2000000002", request.destinationAccount());
    assertEquals("150.00", request.amount());
    assertEquals("USD", request.currency());
    assertEquals("lunch", request.description());
  }

  @Test
  void theLineKeepsItsOwnIdentity() {
    BatchLine line = new BatchLine("line-1", "key-12345678", "2000000001", "2000000002",
        "150.00", "USD", null);

    assertEquals("line-1", line.lineId());
    assertEquals("key-12345678", line.idempotencyKey());
    assertNull(line.description());
  }
}
