package com.acme.transfer.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.acme.transfer.dto.TransferRequest;
import com.acme.transfer.dto.TransferResource;
import com.acme.transfer.repository.TransferEntity;
import com.acme.transfer.service.TransferResult;
import com.acme.transfer.service.TransferService;
import com.acme.transfer.web.ApiException;
import com.acme.transfer.web.Problem;
import com.acme.transfer.web.ProblemWebExceptionHandler;
import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

/**
 * The transfer API end to end at the HTTP boundary: the controller only wires an exchange to the
 * pipeline, so these tests assert the contract it keeps — the status per outcome, and that a
 * failure the pipeline reports becomes an RFC 9457 problem rather than a raw exception.
 */
class TransferControllerTest {

  private static final String KEY = "key-12345678";
  private static final String SOURCE = "2000000001";
  private static final String DESTINATION = "2000000002";

  private TransferService transferService;
  private WebTestClient client;

  @BeforeEach
  void setUp() {
    transferService = mock(TransferService.class);
    client = WebTestClient.bindToController(new TransferController(transferService))
        .controllerAdvice(new ProblemWebExceptionHandler())
        .build();
  }

  private static TransferRequest request() {
    return new TransferRequest(SOURCE, DESTINATION, "10.00", "USD", "lunch");
  }

  private static TransferEntity entity(String transferId, String status, String reasonCode,
                                       String coreTxnId) {
    return new TransferEntity(transferId, status, reasonCode, SOURCE, DESTINATION,
        new BigDecimal("10.00"), "USD", new BigDecimal("160000.00"), "IDR",
        new BigDecimal("16000.00"), coreTxnId, "lunch", Instant.now(), Instant.now());
  }

  private static TransferResult outcome(TransferEntity entity) {
    int httpStatus = switch (entity.status()) {
      case "COMPLETED" -> 201;
      case "PENDING", "PENDING_REVIEW" -> 202;
      default -> 503;
    };
    return new TransferResult(httpStatus, TransferResource.from(entity), null, null);
  }

  @Test
  void aCompletedTransferIsCreated() {
    when(transferService.execute(eq(KEY), any(TransferRequest.class)))
        .thenReturn(Mono.just(outcome(entity("transfer-1", "COMPLETED", null, "CT-1"))));

    client.post().uri("/api/v1/transfers")
        .header("Idempotency-Key", KEY)
        .bodyValue(request())
        .exchange()
        .expectStatus().isCreated()
        .expectBody()
        .jsonPath("$.transferId").isEqualTo("transfer-1")
        .jsonPath("$.status").isEqualTo("COMPLETED")
        .jsonPath("$.sourceAccount").isEqualTo(SOURCE)
        .jsonPath("$.coreTxnId").isEqualTo("CT-1");
  }

  @Test
  void aPendingTransferIsAcceptedNotFailed() {
    when(transferService.execute(eq(KEY), any(TransferRequest.class)))
        .thenReturn(Mono.just(outcome(entity("transfer-2", "PENDING", "CORE_TIMEOUT", null))));

    client.post().uri("/api/v1/transfers")
        .header("Idempotency-Key", KEY)
        .bodyValue(request())
        .exchange()
        .expectStatus().isAccepted()
        .expectBody()
        .jsonPath("$.status").isEqualTo("PENDING")
        .jsonPath("$.reasonCode").isEqualTo("CORE_TIMEOUT");
  }

  @Test
  void aTransferHeldForReviewIsAccepted() {
    when(transferService.execute(eq(KEY), any(TransferRequest.class)))
        .thenReturn(Mono.just(outcome(entity("transfer-3", "PENDING_REVIEW", "FRAUD_UNAVAILABLE",
            null))));

    client.post().uri("/api/v1/transfers")
        .header("Idempotency-Key", KEY)
        .bodyValue(request())
        .exchange()
        .expectStatus().isAccepted()
        .expectBody()
        .jsonPath("$.status").isEqualTo("PENDING_REVIEW");
  }

  @Test
  void aKeyReusedWithAnotherBodyIsUnprocessable() {
    when(transferService.execute(eq(KEY), any(TransferRequest.class)))
        .thenReturn(Mono.just(new TransferResult(422, null,
            ApiException.Codes.IDEMPOTENCY_KEY_REUSED, "The key was used with a different body.")));

    client.post().uri("/api/v1/transfers")
        .header("Idempotency-Key", KEY)
        .bodyValue(request())
        .exchange()
        .expectStatus().isEqualTo(422)
        .expectHeader().contentType(Problem.MEDIA_TYPE)
        .expectBody()
        .jsonPath("$.code").isEqualTo("IDEMPOTENCY_KEY_REUSED")
        .jsonPath("$.status").isEqualTo(422)
        .jsonPath("$.instance").isEqualTo("/api/v1/transfers");
  }

  @Test
  void aRequestAlreadyInProgressIsAConflict() {
    when(transferService.execute(eq(KEY), any(TransferRequest.class)))
        .thenReturn(Mono.just(new TransferResult(409, null,
            ApiException.Codes.REQUEST_IN_PROGRESS, "A request with that key is in progress.")));

    client.post().uri("/api/v1/transfers")
        .header("Idempotency-Key", KEY)
        .bodyValue(request())
        .exchange()
        .expectStatus().isEqualTo(409)
        .expectHeader().contentType(Problem.MEDIA_TYPE)
        .expectBody()
        .jsonPath("$.code").isEqualTo("REQUEST_IN_PROGRESS");
  }

  @Test
  void aTransferIsFoundById() {
    when(transferService.getTransfer("transfer-1"))
        .thenReturn(Mono.just(entity("transfer-1", "COMPLETED", null, "CT-1")));

    client.get().uri("/api/v1/transfers/transfer-1")
        .exchange()
        .expectStatus().isOk()
        .expectBody()
        .jsonPath("$.transferId").isEqualTo("transfer-1")
        .jsonPath("$.status").isEqualTo("COMPLETED");
  }

  @Test
  void anUnknownTransferIsNotFound() {
    when(transferService.getTransfer("transfer-9")).thenReturn(Mono.empty());

    client.get().uri("/api/v1/transfers/transfer-9")
        .exchange()
        .expectStatus().isNotFound()
        .expectHeader().contentType(Problem.MEDIA_TYPE)
        .expectBody()
        .jsonPath("$.code").isEqualTo("TRANSFER_NOT_FOUND")
        .jsonPath("$.detail").isEqualTo("Transfer transfer-9 was not found.");

    verify(transferService).getTransfer("transfer-9");
  }
}
