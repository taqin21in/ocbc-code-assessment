package com.acme.transfer.web;

import lombok.Getter;
import org.springframework.http.HttpStatus;

/**
 * An error the transfer API knows how to explain: a validation failure, an idempotency conflict,
 * a missing transfer, a reconciliation already running, …
 *
 * <p>It carries an RFC 9457 {@link Problem} rather than an HTTP status alone: the {@code code} is
 * what the web client and support use to recognise the failure class. Unexpected failures never
 * use this class — they are bugs and become {@code INTERNAL_ERROR}.
 */
@Getter
public class ApiException extends RuntimeException {

  private final int statusCode;
  private final String code;

  public ApiException(HttpStatus status, String code, String detail, Throwable cause) {
    super(detail, cause);
    this.statusCode = status.value();
    this.code = code;
  }

  public ApiException(HttpStatus status, String code, String detail) {
    this(status, code, detail, null);
  }

  /** Status code, in the name of every place that compares failures instead of exceptions. */
  public interface Codes {

    String VALIDATION_ERROR = "VALIDATION_ERROR";
    String TRANSFER_NOT_FOUND = "TRANSFER_NOT_FOUND";
    String REQUEST_IN_PROGRESS = "REQUEST_IN_PROGRESS";
    String IDEMPOTENCY_KEY_REUSED = "IDEMPOTENCY_KEY_REUSED";
    String RECONCILIATION_RUNNING = "RECONCILIATION_RUNNING";
    String INTERNAL_ERROR = "INTERNAL_ERROR";
  }
}
