package com.acme.transfer.service;

import java.time.Instant;

/**
 * Thrown when the only rate available is past its validity, so quoting it would move money at a
 * price nobody offered. Failing here beats converting at a stale rate; a later retry is safe.
 */
public class ExpiredRateException extends RuntimeException {

  public ExpiredRateException(String base, String quote, Instant validUntil, Instant now) {
    super("FX rate " + base + "/" + quote + " expired at " + validUntil + " (now " + now + ")");
  }
}
