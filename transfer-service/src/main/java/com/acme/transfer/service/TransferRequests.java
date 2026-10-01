package com.acme.transfer.service;

import com.acme.transfer.dto.TransferRequest;
import java.math.BigDecimal;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Request validation shared by the single transfer endpoint and every batch line.
 *
 * <p>Kept out of the controller so the two entry points cannot drift apart, and returning the
 * first failure as a message instead of an exception so the caller decides how to report it
 * (problem detail for the API, an error result for a batch line).
 */
public final class TransferRequests {

  /** The API contract: 8 to 64 letters, digits, underscore or hyphen. */
  public static final Pattern IDEMPOTENCY_KEY = Pattern.compile("[\\p{Alnum}_-]{8,64}");

  static final Set<String> CURRENCIES = Set.of("IDR", "USD", "SGD");

  static final BigDecimal MAX_AMOUNT = new BigDecimal("1000000000.00");

  private TransferRequests() {
  }

  /** Null when the key satisfies the contract, the reason for the 400 otherwise. */
  public static String validateIdempotencyKey(String key) {
    if (key == null || !IDEMPOTENCY_KEY.matcher(key).matches()) {
      return "Idempotency-Key must be 8 to 64 letters, digits, underscore or hyphen.";
    }
    return null;
  }

  /** Null when the request satisfies the contract, the reason for the 400 otherwise. */
  public static String validate(TransferRequest request) {
    if (request == null) {
      return "The request body is required.";
    }
    if (request.sourceAccount() == null || !request.sourceAccount().matches("\\d{10}")) {
      return "sourceAccount must be 10 digits.";
    }
    if (request.destinationAccount() == null
        || !request.destinationAccount().matches("\\d{10}")) {
      return "destinationAccount must be 10 digits.";
    }
    if (request.sourceAccount().equals(request.destinationAccount())) {
      return "sourceAccount and destinationAccount must differ.";
    }
    String amountProblem = validateAmount(request.amount());
    if (amountProblem != null) {
      return amountProblem;
    }
    if (request.currency() == null || !CURRENCIES.contains(request.currency())) {
      return "currency must be IDR, USD or SGD.";
    }
    if (request.description() != null && request.description().length() > 140) {
      return "description must be at most 140 characters.";
    }
    return null;
  }

  static String validateAmount(String amount) {
    BigDecimal parsed;
    try {
      parsed = new BigDecimal(amount);
    } catch (NumberFormatException | NullPointerException e) {
      return "amount must be a decimal string.";
    }
    if (parsed.signum() <= 0) {
      return "amount must be greater than 0.";
    }
    if (parsed.scale() > 2) {
      return "amount must have at most 2 decimal places.";
    }
    if (parsed.compareTo(MAX_AMOUNT) > 0) {
      return "amount must be at most 1000000000.00.";
    }
    return null;
  }
}
