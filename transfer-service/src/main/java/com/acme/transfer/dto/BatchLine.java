package com.acme.transfer.dto;

/**
 * One line of a batch upload. A {@link TransferRequest} with the two things the batch protocol
 * adds: the caller's {@code lineId}, echoed in the results, and the key that deduplicates the line.
 */
public record BatchLine(String lineId, String idempotencyKey, String sourceAccount,
                        String destinationAccount, String amount, String currency,
                        String description) {

  /** The transfer request this line carries, for the transfer pipeline. */
  public TransferRequest toRequest() {
    return new TransferRequest(sourceAccount, destinationAccount, amount, currency, description);
  }
}
