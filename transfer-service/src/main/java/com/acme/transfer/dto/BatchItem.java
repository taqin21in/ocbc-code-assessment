package com.acme.transfer.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * One line of the request body, parsed but not yet validated, and where its result belongs in the
 * response stream.
 */
public sealed interface BatchItem permits BatchItem.Work, BatchItem.Failure {

  /** A line that parses and validates, and so must go through the transfer pipeline. */
  record Work(int lineNumber, String lineId, String idempotencyKey, String sourceAccount,
              TransferRequest request) implements BatchItem {
  }

  /** A line that cannot run, with the reason its result line will carry. */
  record Failure(int lineNumber, String lineId, String code, String message) implements BatchItem {
  }
}
