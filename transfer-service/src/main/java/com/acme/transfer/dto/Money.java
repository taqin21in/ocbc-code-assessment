package com.acme.transfer.dto;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Money on the wire: a two-place decimal string and its currency. Money internal to the service is
 * {@link BigDecimal}; this is only the last stop, where the amount is normalised to the two places
 * the API contract promises, with the rounding it specifies.
 */
public record Money(String amount, String currency) {

  public static Money of(BigDecimal amount, String currency) {
    if (amount == null || currency == null) {
      return null;
    }
    return new Money(amount.setScale(2, RoundingMode.HALF_EVEN).toPlainString(), currency);
  }
}
