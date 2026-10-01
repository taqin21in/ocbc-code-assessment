package com.acme.transfer.service;

import com.acme.transfer.dto.TransferRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * A deterministic fingerprint of a transfer request, for idempotency.
 *
 * <p>Not {@code request.hashCode()}: a JVM hash code is not stable across runs or replicas, is
 * collision prone, and says nothing about what the request actually is. This is the canonical JSON
 * of the request, SHA-256 hashed: two identical bodies always hash alike, two different bodies
 * never do in practice, and the same value is reproducible after a restart or on another node.
 */
public final class RequestFingerprint {

  private static final HexFormat HEX = HexFormat.of();

  private RequestFingerprint() {
  }

  /** SHA-256 of the request's canonical JSON, as 64 lowercase hex characters. */
  public static String sha256(TransferRequest request, ObjectMapper objectMapper) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      return HEX.formatHex(digest.digest(objectMapper.writeValueAsBytes(request)));
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("The request cannot be fingerprinted", e);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }
}
