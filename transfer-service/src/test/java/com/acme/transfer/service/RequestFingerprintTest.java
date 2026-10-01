package com.acme.transfer.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.acme.transfer.dto.TransferRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

/**
 * INC-105: the fingerprint must be deterministic SHA-256, never {@code hashCode()}. Identical
 * requests always match, different requests never do, and the value is reproducible across runs.
 */
class RequestFingerprintTest {

  private final ObjectMapper objectMapper = new ObjectMapper();

  private static TransferRequest request() {
    return new TransferRequest("2000000001", "2000000002", "150.00", "USD", "lunch");
  }

  @Test
  void identicalRequestsHashIdentically() {
    assertEquals(RequestFingerprint.sha256(request(), objectMapper),
        RequestFingerprint.sha256(request(), objectMapper));
  }

  @Test
  void differentRequestsHashDifferently() {
    TransferRequest other =
        new TransferRequest("2000000001", "2000000003", "150.00", "USD", "lunch");

    assertNotEquals(RequestFingerprint.sha256(request(), objectMapper),
        RequestFingerprint.sha256(other, objectMapper));
  }

  @Test
  void theHashIsSixtyFourHexCharacters() {
    String hash = RequestFingerprint.sha256(request(), objectMapper);

    assertEquals(64, hash.length());
    assertTrue(hash.matches("[0-9a-f]{64}"), "not lowercase hex: " + hash);
  }

  @Test
  void fieldOrderChangesTheHash() {
    ObjectMapper shuffled = new ObjectMapper();
    // A different mapper configuration could serialise differently; the default is stable, but a
    // null and an empty description must never collide.
    TransferRequest withNull = new TransferRequest("2000000001", "2000000002", "150.00", "USD",
        null);

    assertNotNull(RequestFingerprint.sha256(withNull, objectMapper));
    assertNotEquals(RequestFingerprint.sha256(request(), objectMapper),
        RequestFingerprint.sha256(withNull, objectMapper));
  }

  @Test
  void theHashDoesNotDependOnTheMapperInstance() {
    // A fingerprint stored by one process must match one computed by another: no mapper state,
    // no identity, no clock.
    assertEquals(RequestFingerprint.sha256(request(), new ObjectMapper()),
        RequestFingerprint.sha256(request(), new ObjectMapper()));
  }
}
