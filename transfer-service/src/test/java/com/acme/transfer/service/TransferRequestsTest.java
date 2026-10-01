package com.acme.transfer.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.acme.transfer.dto.TransferRequest;
import org.junit.jupiter.api.Test;

/**
 * Request validation, shared by the single endpoint and every batch line. Every rule here becomes
 * either a 400 problem detail or, in a batch, one failed line.
 */
class TransferRequestsTest {

  private static TransferRequest valid() {
    return new TransferRequest("2000000001", "2000000002", "150.00", "USD", "lunch");
  }

  @Test
  void aValidRequestHasNoProblem() {
    assertNull(TransferRequests.validate(valid()));
  }

  @Test
  void aValidIdempotencyKeyHasNoProblem() {
    assertNull(TransferRequests.validateIdempotencyKey("order-2024-01"));
  }

  @Test
  void aNullBodyIsRejected() {
    assertEquals("The request body is required.", TransferRequests.validate(null));
  }

  @Test
  void accountNumbersMustBeTenDigits() {
    TransferRequest shortAccount =
        new TransferRequest("123", "2000000002", "150.00", "USD", "lunch");
    assertEquals("sourceAccount must be 10 digits.", TransferRequests.validate(shortAccount));

    TransferRequest letters =
        new TransferRequest("200000000a", "2000000002", "150.00", "USD", "lunch");
    assertEquals("sourceAccount must be 10 digits.", TransferRequests.validate(letters));

    TransferRequest badDestination =
        new TransferRequest("2000000001", "20000000 2", "150.00", "USD", "lunch");
    assertEquals("destinationAccount must be 10 digits.", TransferRequests.validate(badDestination));
  }

  @Test
  void nullAccountsAreRejected() {
    TransferRequest request = new TransferRequest(null, null, "150.00", "USD", "lunch");
    assertEquals("sourceAccount must be 10 digits.", TransferRequests.validate(request));
  }

  @Test
  void sourceAndDestinationMustDiffer() {
    TransferRequest same = new TransferRequest("2000000001", "2000000001", "150.00", "USD",
        "lunch");
    assertEquals("sourceAccount and destinationAccount must differ.",
        TransferRequests.validate(same));
  }

  @Test
  void amountsMustBePositiveDecimalsWithTwoPlaces() {
    assertEquals("amount must be a decimal string.",
        TransferRequests.validate(new TransferRequest("2000000001", "2000000002", "free", "USD",
            "lunch")));
    assertEquals("amount must be greater than 0.",
        TransferRequests.validate(new TransferRequest("2000000001", "2000000002", "0.00", "USD",
            "lunch")));
    assertEquals("amount must be greater than 0.",
        TransferRequests.validate(new TransferRequest("2000000001", "2000000002", "-10.00", "USD",
            "lunch")));
    assertEquals("amount must have at most 2 decimal places.",
        TransferRequests.validate(new TransferRequest("2000000001", "2000000002", "10.005", "USD",
            "lunch")));
  }

  @Test
  void amountsAreBounded() {
    assertEquals("amount must be at most 1000000000.00.",
        TransferRequests.validate(new TransferRequest("2000000001", "2000000002",
            "1000000000.01", "USD", "lunch")));
    assertNull(TransferRequests.validate(new TransferRequest("2000000001", "2000000002",
        "1000000000.00", "USD", "lunch")));
  }

  @Test
  void currencyIsLimitedToTheThreeSupported() {
    assertEquals("currency must be IDR, USD or SGD.",
        TransferRequests.validate(new TransferRequest("2000000001", "2000000002", "150.00", "EUR",
            "lunch")));
    assertEquals("currency must be IDR, USD or SGD.",
        TransferRequests.validate(new TransferRequest("2000000001", "2000000002", "150.00", null,
            "lunch")));
  }

  @Test
  void descriptionsAreBounded() {
    String tooLong = "x".repeat(141);
    assertEquals("description must be at most 140 characters.",
        TransferRequests.validate(new TransferRequest("2000000001", "2000000002", "150.00", "USD",
            tooLong)));
    assertNull(TransferRequests.validate(new TransferRequest("2000000001", "2000000002", "150.00",
        "USD", "x".repeat(140))));
  }

  @Test
  void idempotencyKeysAreBoundedAndAlphanumeric() {
    assertNotNull(TransferRequests.validateIdempotencyKey(null));
    assertNotNull(TransferRequests.validateIdempotencyKey("short"));
    assertNotNull(TransferRequests.validateIdempotencyKey("has spaces"));
    assertNotNull(TransferRequests.validateIdempotencyKey("x".repeat(65)));
    assertNull(TransferRequests.validateIdempotencyKey("x".repeat(64)));
    assertNull(TransferRequests.validateIdempotencyKey("order_2024-01"));
  }
}
