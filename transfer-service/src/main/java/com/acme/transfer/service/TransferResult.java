package com.acme.transfer.service;

import com.acme.transfer.dto.TransferResource;
import com.acme.transfer.repository.TransferEntity;

/**
 * The answer of one transfer attempt, for the single endpoint and for batch lines alike.
 *
 * @param httpStatus the status the API contract assigns to this outcome
 * @param transfer the transfer resource, when the pipeline produced one
 * @param code the machine readable failure code, when it did not
 * @param detail the human readable failure detail, when it did not
 */
public record TransferResult(int httpStatus, TransferResource transfer, String code, String detail) {

  /** A completed business outcome: the transfer exists and is returned. */
  static TransferResult of(TransferEntity entity) {
    return new TransferResult(httpStatus(entity.status()), TransferResource.from(entity), null,
        null);
  }

  /** A failure that produced no transfer: a validation, key reuse or in-progress answer. */
  static TransferResult problem(int httpStatus, String code, String detail) {
    return new TransferResult(httpStatus, null, code, detail);
  }

  /** The contract's status per outcome. */
  static int httpStatus(String status) {
    return switch (status) {
      case "COMPLETED" -> 201;
      case "PENDING_REVIEW", "PENDING" -> 202;
      case "REJECTED" -> 422;
      default -> 503;
    };
  }
}
