package com.acme.transfer.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * One result line of a batch upload, in completion order.
 *
 * <p>{@code lineId} is always present, null when the line could not even be parsed; every other
 * field only appears when it applies, so a success carries a {@code transfer} and a failure an
 * {@code error}.
 */
public record BatchLineResult(String lineId,
                              @JsonInclude(JsonInclude.Include.NON_NULL) Integer lineNumber,
                              int httpStatus,
                              @JsonInclude(JsonInclude.Include.NON_NULL) TransferResource transfer,
                              @JsonInclude(JsonInclude.Include.NON_NULL) BatchError error) {

  public static BatchLineResult ok(String lineId, int httpStatus, TransferResource transfer) {
    return new BatchLineResult(lineId, null, httpStatus, transfer, null);
  }

  public static BatchLineResult failure(String lineId, Integer lineNumber, int httpStatus,
                                        String code, String message) {
    return new BatchLineResult(lineId, lineNumber, httpStatus, null, new BatchError(code, message));
  }

  /** The machine readable code and the human readable message of one failed line. */
  public record BatchError(String code, String message) {
  }
}
