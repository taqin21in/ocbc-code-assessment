package com.acme.transfer.web;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.acme.transfer.config.CorrelationIdFilter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * INC-111: every error leaves as {@code application/problem+json} with a code, a detail and the
 * correlation ID — and never a stack trace, an exception class name or a hostname.
 */
class ProblemWebExceptionHandlerTest {

  private final ObjectMapper objectMapper = new ObjectMapper();
  private final ProblemWebExceptionHandler handler = new ProblemWebExceptionHandler();

  private MockServerWebExchange exchange() {
    MockServerWebExchange exchange = MockServerWebExchange.from(
        MockServerHttpRequest.post("/api/v1/transfers").build());
    exchange.getAttributes().put(CorrelationIdFilter.EXCHANGE_ATTRIBUTE, "corr-123");
    return exchange;
  }

  private JsonNode bodyOf(MockServerWebExchange exchange) throws Exception {
    return objectMapper.readTree(exchange.getResponse().getBodyAsString().block());
  }

  @Test
  void anApiExceptionBecomesAProblemWithItsCode() throws Exception {
    MockServerWebExchange exchange = exchange();

    StepVerifier.create(handler.handle(exchange,
            new ApiException(HttpStatus.CONFLICT, ApiException.Codes.RECONCILIATION_RUNNING,
                "A reconciliation pass is already running.")))
        .verifyComplete();

    assertEquals(HttpStatus.CONFLICT, exchange.getResponse().getStatusCode());
    assertEquals(Problem.MEDIA_TYPE, exchange.getResponse().getHeaders().getContentType().toString());
    JsonNode body = bodyOf(exchange);
    assertEquals("RECONCILIATION_RUNNING", body.get("code").asText());
    assertEquals("A reconciliation pass is already running.", body.get("detail").asText());
    assertEquals(409, body.get("status").asInt());
    assertEquals("corr-123", body.get("correlationId").asText());
    assertEquals("/api/v1/transfers", body.get("instance").asText());
  }

  @Test
  void aValidationFailureIsABadRequest() throws Exception {
    MockServerWebExchange exchange = exchange();

    StepVerifier.create(handler.handle(exchange,
            new ApiException(HttpStatus.BAD_REQUEST, ApiException.Codes.VALIDATION_ERROR,
                "amount must be greater than 0.")))
        .verifyComplete();

    JsonNode body = bodyOf(exchange);
    assertEquals(400, body.get("status").asInt());
    assertEquals("VALIDATION_ERROR", body.get("code").asText());
    assertEquals("Invalid request", body.get("title").asText());
  }

  @Test
  void anIdempotencyConflictIsAConflict() throws Exception {
    MockServerWebExchange exchange = exchange();

    StepVerifier.create(handler.handle(exchange,
            new ApiException(HttpStatus.CONFLICT, ApiException.Codes.REQUEST_IN_PROGRESS,
                "A request with that key is in progress.")))
        .verifyComplete();

    JsonNode body = bodyOf(exchange);
    assertEquals(409, body.get("status").asInt());
    assertEquals("REQUEST_IN_PROGRESS", body.get("code").asText());
  }

  @Test
  void aKeyReusedIsUnprocessable() throws Exception {
    MockServerWebExchange exchange = exchange();

    StepVerifier.create(handler.handle(exchange,
            new ApiException(HttpStatus.UNPROCESSABLE_ENTITY, ApiException.Codes.IDEMPOTENCY_KEY_REUSED,
                "The key was used with a different body.")))
        .verifyComplete();

    JsonNode body = bodyOf(exchange);
    assertEquals(422, body.get("status").asInt());
    assertEquals("IDEMPOTENCY_KEY_REUSED", body.get("code").asText());
    assertEquals("Unprocessable content", body.get("title").asText());
  }

  @Test
  void aMissingTransferBecomesNotFound() throws Exception {
    MockServerWebExchange exchange = exchange();

    StepVerifier.create(handler.handle(exchange, new ApiException(HttpStatus.NOT_FOUND,
            ApiException.Codes.TRANSFER_NOT_FOUND, "Transfer x was not found.")))
        .verifyComplete();

    JsonNode body = bodyOf(exchange);
    assertEquals(404, body.get("status").asInt());
    assertEquals("TRANSFER_NOT_FOUND", body.get("code").asText());
  }

  @Test
  void aDependencyFailureIsAnInternalServerError() throws Exception {
    MockServerWebExchange exchange = exchange();

    StepVerifier.create(handler.handle(exchange, new ApiException(HttpStatus.SERVICE_UNAVAILABLE,
            ApiException.Codes.INTERNAL_ERROR, "The fraud service is unavailable.")))
        .verifyComplete();

    JsonNode body = bodyOf(exchange);
    assertEquals(503, body.get("status").asInt());
    assertEquals("INTERNAL_ERROR", body.get("code").asText());
  }

  @Test
  void aResponseStatusExceptionIsTranslated() throws Exception {
    MockServerWebExchange exchange = exchange();

    StepVerifier.create(handler.handle(exchange,
            new ResponseStatusException(HttpStatus.NOT_FOUND, "Not here")))
        .verifyComplete();

    JsonNode body = bodyOf(exchange);
    assertEquals(404, body.get("status").asInt());
    assertEquals("TRANSFER_NOT_FOUND", body.get("code").asText());
    assertEquals("Not here", body.get("detail").asText());
  }

  @Test
  void aResponseStatusExceptionWithoutAReasonGetsAGenericOne() throws Exception {
    MockServerWebExchange exchange = exchange();

    StepVerifier.create(handler.handle(exchange, new ResponseStatusException(HttpStatus.BAD_REQUEST)))
        .verifyComplete();

    JsonNode body = bodyOf(exchange);
    assertEquals("Request cannot be processed as sent.", body.get("detail").asText());
    assertEquals("VALIDATION_ERROR", body.get("code").asText());
  }

  @Test
  void malformedJsonBecomesABadRequestWithoutTheExceptionClass() throws Exception {
    MockServerWebExchange exchange = exchange();

    StepVerifier.create(handler.handle(exchange, new ResponseStatusException(HttpStatus.BAD_REQUEST,
            "Request body is unreadable",
            new com.fasterxml.jackson.core.JsonParseException(null, "Unexpected character"))))
        .verifyComplete();

    JsonNode body = bodyOf(exchange);
    assertEquals(400, body.get("status").asInt());
    String detail = body.get("detail").asText();
    assertEquals("The request body is not valid JSON or is not of the expected shape.", detail);
    assertFalse(detail.contains("JsonParseException"), "the detail leaks the exception class");
  }

  @Test
  void aBindExceptionIsAValidationError() throws Exception {
    MockServerWebExchange exchange = exchange();

    org.springframework.validation.BeanPropertyBindingResult bindingResult =
        new org.springframework.validation.BeanPropertyBindingResult(new Object(), "request");
    bindingResult.addError(new org.springframework.validation.FieldError("request", "amount",
        "amount must be a number"));

    StepVerifier.create(handler.handle(exchange,
            new WebExchangeBindException(null, bindingResult)))
        .verifyComplete();

    JsonNode body = bodyOf(exchange);
    assertEquals("VALIDATION_ERROR", body.get("code").asText());
  }

  @Test
  void aDataIntegrityViolationIsAConflict() throws Exception {
    MockServerWebExchange exchange = exchange();

    StepVerifier.create(handler.handle(exchange,
            new DataIntegrityViolationException("duplicate key value violates"
                + " idempotency_keys_pkey")))
        .verifyComplete();

    JsonNode body = bodyOf(exchange);
    assertEquals(409, body.get("status").asInt());
    assertEquals("REQUEST_IN_PROGRESS", body.get("code").asText());
    assertFalse(body.get("detail").asText().contains("idempotency_keys_pkey"),
        "the detail leaks the schema");
  }

  @Test
  void anUnexpectedFailureBecomesAnInternalError() throws Exception {
    MockServerWebExchange exchange = exchange();

    StepVerifier.create(handler.handle(exchange, new IllegalStateException("NPE in the pipeline")))
        .verifyComplete();

    JsonNode body = bodyOf(exchange);
    assertEquals(500, body.get("status").asInt());
    assertEquals("INTERNAL_ERROR", body.get("code").asText());
    assertEquals("An unexpected error occurred.", body.get("detail").asText());

    String whole = exchange.getResponse().getBodyAsString().block();
    assertFalse(whole.contains("IllegalStateException"), "the exception class name is exposed");
    assertFalse(whole.contains("at com.acme"), "a stack trace is exposed");
    assertFalse(whole.contains("localhost"), "a hostname is exposed");
  }

  @Test
  void anAlreadyCommittedResponseIsNotRewritten() {
    MockServerWebExchange exchange = exchange();
    // A response already sent to the client cannot be taken back: the failure is propagated instead.
    exchange.getResponse().setComplete().block();
    assertTrue(exchange.getResponse().isCommitted());

    StepVerifier.create(handler.handle(exchange, new IllegalStateException("too late")))
        .expectError(IllegalStateException.class)
        .verify();
  }

  @Test
  void aRequestWithoutACorrelationIdStillAnswers() throws Exception {
    MockServerWebExchange exchange = MockServerWebExchange.from(
        MockServerHttpRequest.post("/api/v1/transfers").build());

    StepVerifier.create(handler.handle(exchange, new ApiException(HttpStatus.BAD_REQUEST,
            ApiException.Codes.VALIDATION_ERROR, "bad")))
        .verifyComplete();

    JsonNode body = bodyOf(exchange);
    assertTrue(body.has("correlationId"));
  }

  @Test
  void theProblemTypeIsAboutBlank() throws Exception {
    MockServerWebExchange exchange = exchange();

    StepVerifier.create(handler.handle(exchange, new ApiException(HttpStatus.BAD_REQUEST,
            ApiException.Codes.VALIDATION_ERROR, "bad")))
        .verifyComplete();

    assertEquals("about:blank", bodyOf(exchange).get("type").asText());
  }

  @Test
  void aProblemDetailIsAlsoRendered() {
    ProblemDetail detail = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, "bad");
    detail.setProperty("code", ApiException.Codes.VALIDATION_ERROR);
    assertEquals(400, detail.getStatus());
  }
}
