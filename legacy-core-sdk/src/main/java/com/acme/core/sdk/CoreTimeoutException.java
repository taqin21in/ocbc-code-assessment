package com.acme.core.sdk;

/**
 * The request was sent, but no response arrived in time or the connection was lost.
 *
 * <p>The outcome is unknown: the core may or may not have processed the request. Use
 * {@link CoreBankingClient#inquire(String)} to find out.
 */
public class CoreTimeoutException extends CoreBankingException {

  public CoreTimeoutException(String message) {
    super(message);
  }

  public CoreTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }

  public CoreTimeoutException(String operation, String baseUrl, int timeoutMillis,
                              Throwable cause) {
    super(operation + ": no response from " + baseUrl + " within " + timeoutMillis + " ms", cause);
  }
}
