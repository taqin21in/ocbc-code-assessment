package com.acme.transfer.web;

import com.acme.transfer.config.CorrelationIdFilter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebExceptionHandler;
import reactor.core.publisher.Mono;

/**
 * The only error renderer of the API.
 *
 * <p>Every failure — validation, malformed JSON, a missing transfer, an idempotency conflict, a
 * dependency that is down, a reconciliation already running and the failures nobody expected —
 * leaves here as {@code application/problem+json} with a {@code code}, a {@code detail} and the
 * request's correlation ID. Nothing but the detail is ever derived from an exception: no stack
 * traces, no exception class names, no internal hostnames, no credentials. The exception itself is
 * logged, with the correlation ID, so support finds it there instead.
 */
@Slf4j
@Order(-2)
public class ProblemWebExceptionHandler implements WebExceptionHandler {

  private static final ObjectMapper JSON = new ObjectMapper();

  @Override
  public Mono<Void> handle(ServerWebExchange exchange, Throwable error) {
    if (exchange.getResponse().isCommitted()) {
      return Mono.error(error);
    }

    HttpStatusCode status = resolveStatus(error);
    String code = resolveCode(error);
    String detail = resolveDetail(error, status);
    String title = title(status);
    String instance = exchange.getRequest().getPath().pathWithinApplication().value();
    String correlationId = (String) exchange.getAttribute(CorrelationIdFilter.EXCHANGE_ATTRIBUTE);
    Problem problem = new Problem("about:blank", title, status.value(), detail, instance, code,
        correlationId);

    if (status.is5xxServerError()) {
      logFailure(exchange, instance, status, code, detail, error, true);
    } else {
      logFailure(exchange, instance, status, code, detail, error, false);
    }

    exchange.getResponse().setStatusCode(status);
    exchange.getResponse().getHeaders().setContentType(
        MediaType.parseMediaType(Problem.MEDIA_TYPE));
    return write(exchange, problem);
  }

  private HttpStatusCode resolveStatus(Throwable error) {
    if (error instanceof ApiException api) {
      return HttpStatusCode.valueOf(api.getStatusCode());
    }
    if (error instanceof ResponseStatusException statusError
        && statusError.getStatusCode() != null) {
      return statusError.getStatusCode();
    }
    if (error instanceof DataIntegrityViolationException) {
      return HttpStatus.CONFLICT;
    }
    return HttpStatus.INTERNAL_SERVER_ERROR;
  }

  private String resolveCode(Throwable error) {
    if (error instanceof ApiException api) {
      return api.getCode();
    }
    if (error instanceof ResponseStatusException statusError
        && statusError.getStatusCode() != null) {
      HttpStatusCode status = statusError.getStatusCode();
      if (status.value() == HttpStatus.NOT_FOUND.value()) {
        return ApiException.Codes.TRANSFER_NOT_FOUND;
      }
      if (status.value() == HttpStatus.CONFLICT.value()) {
        return ApiException.Codes.REQUEST_IN_PROGRESS;
      }
      if (status.is5xxServerError()) {
        return ApiException.Codes.INTERNAL_ERROR;
      }
      return ApiException.Codes.VALIDATION_ERROR;
    }
    if (error instanceof DataIntegrityViolationException) {
      return ApiException.Codes.REQUEST_IN_PROGRESS;
    }
    return ApiException.Codes.INTERNAL_ERROR;
  }

  /** The detail shown to the caller: never the exception class, never a stack, never a host. */
  private String resolveDetail(Throwable error, HttpStatusCode status) {
    if (error instanceof ApiException api) {
      return api.getMessage();
    }
    // A malformed body is reported with a stable message before the generic status reason, so the
    // parser's own wording (which mentions the exception class) never reaches the caller.
    if (isMalformedBody(error)) {
      return "The request body is not valid JSON or is not of the expected shape.";
    }
    if (error instanceof ResponseStatusException statusError) {
      return statusError.getReason() != null && !statusError.getReason().isBlank()
          ? statusError.getReason() : "Request cannot be processed as sent.";
    }
    if (error instanceof DataIntegrityViolationException) {
      return "The request conflicts with one that is already in progress.";
    }
    if (status.value() == HttpStatus.NOT_FOUND.value()) {
      return "The requested resource was not found.";
    }
    return "An unexpected error occurred.";
  }

  private boolean isMalformedBody(Throwable error) {
    return error instanceof JsonProcessingException
        || (error.getCause() instanceof JsonProcessingException);
  }

  private String title(HttpStatusCode status) {
    if (status.value() == HttpStatus.BAD_REQUEST.value()) {
      return "Invalid request";
    }
    if (status.value() == HttpStatus.UNPROCESSABLE_ENTITY.value()) {
      return "Unprocessable content";
    }
    HttpStatus resolved = HttpStatus.resolve(status.value());
    return resolved == null ? "Request failed" : resolved.getReasonPhrase();
  }

  /**
   * Records the failure, with the correlation ID, so support finds it there instead of in the
   * response body. Rendering an error must never itself fail: some exceptions report a message that
   * blows up (a bind exception without a method parameter, for example), so the exception is only
   * attached when the framework can actually read it.
   */
  private void logFailure(ServerWebExchange exchange, String instance, HttpStatusCode status,
                          String code, String detail, Throwable error, boolean serverError) {
    try {
      if (serverError) {
        log.error("{} {} failed as {} [{}]", exchange.getRequest().getMethod(),
            instance, status.value(), code, error);
      } else {
        log.warn("{} {} rejected as {} [{}]: {}", exchange.getRequest().getMethod(),
            instance, status.value(), code, detail, error);
      }
    } catch (RuntimeException loggingFailure) {
      log.warn("{} {} rejected as {} [{}]", exchange.getRequest().getMethod(),
          instance, status.value(), code);
    }
  }

  private Mono<Void> write(ServerWebExchange exchange, Problem problem) {
    byte[] body;
    try {
      body = JSON.writeValueAsBytes(problem);
    } catch (JsonProcessingException jsonError) {
      log.error("Could not serialize problem, returning a minimal error body", jsonError);
      exchange.getResponse().setStatusCode(HttpStatus.INTERNAL_SERVER_ERROR);
      body = ("{\"type\":\"about:blank\",\"title\":\"Internal error\",\"status\":500,"
          + "\"code\":\"INTERNAL_ERROR\"}").getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
    DataBuffer buffer = exchange.getResponse().bufferFactory().wrap(body);
    return exchange.getResponse().writeWith(Mono.just(buffer));
  }
}
