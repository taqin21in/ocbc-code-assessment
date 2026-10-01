package com.acme.transfer.dto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

/** Result lines carry a success or an error, never both, and always their {@code lineId}. */
class BatchLineResultTest {

  private static final TransferResource TRANSFER = new TransferResource("transfer-1", "COMPLETED",
      null, "2000000001", "2000000002", null, null, null, "CT1", null, null);

  @Test
  void aSuccessCarriesTheTransferAndNoError() {
    BatchLineResult result = BatchLineResult.ok("line-1", 201, TRANSFER);

    assertEquals("line-1", result.lineId());
    assertEquals(201, result.httpStatus());
    assertEquals(TRANSFER, result.transfer());
    assertNull(result.error());
    assertNull(result.lineNumber());
  }

  @Test
  void aFailureCarriesTheErrorAndNoTransfer() {
    BatchLineResult result = BatchLineResult.failure("line-1", 7, 422,
        ApiExceptionCodes.IDEMPOTENCY_KEY_REUSED, "used with another body");

    assertEquals("line-1", result.lineId());
    assertEquals(422, result.httpStatus());
    assertEquals(7, result.lineNumber());
    assertEquals("IDEMPOTENCY_KEY_REUSED", result.error().code());
    assertEquals("used with another body", result.error().message());
    assertNull(result.transfer());
  }

  @Test
  void anUnparseableLineHasNoLineNumberButKeepsItsPosition() {
    BatchLineResult result = BatchLineResult.failure(null, 3, 400,
        ApiExceptionCodes.VALIDATION_ERROR, "not valid JSON");

    assertNull(result.lineId());
    assertEquals(3, result.lineNumber());
    assertEquals("VALIDATION_ERROR", result.error().code());
  }

  private static final class ApiExceptionCodes {
    static final String IDEMPOTENCY_KEY_REUSED = "IDEMPOTENCY_KEY_REUSED";
    static final String VALIDATION_ERROR = "VALIDATION_ERROR";
  }
}
