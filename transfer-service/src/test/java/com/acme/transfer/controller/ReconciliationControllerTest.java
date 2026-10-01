package com.acme.transfer.controller;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.acme.transfer.service.ReconciliationService;
import com.acme.transfer.service.ReconciliationStats;
import com.acme.transfer.web.ApiException;
import com.acme.transfer.web.Problem;
import com.acme.transfer.web.ProblemWebExceptionHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

/**
 * FEAT-202: the manual reconciliation trigger. The controller hands the contract's stats back;
 * the 409 the service raises when a pass is already running must reach the caller as a problem.
 */
class ReconciliationControllerTest {

  private ReconciliationService reconciliationService;
  private WebTestClient client;

  @BeforeEach
  void setUp() {
    reconciliationService = mock(ReconciliationService.class);
    client = WebTestClient.bindToController(new ReconciliationController(reconciliationService))
        .controllerAdvice(new ProblemWebExceptionHandler())
        .build();
  }

  @Test
  void aFinishedRunReportsItsStats() {
    when(reconciliationService.runOnce())
        .thenReturn(Mono.just(new ReconciliationStats(12, 7, 1, 2, 2, 1840)));

    client.post().uri("/internal/reconciliation/run")
        .exchange()
        .expectStatus().isOk()
        .expectBody()
        .jsonPath("$.checked").isEqualTo(12)
        .jsonPath("$.completed").isEqualTo(7)
        .jsonPath("$.rejected").isEqualTo(1)
        .jsonPath("$.failed").isEqualTo(2)
        .jsonPath("$.stillPending").isEqualTo(2)
        .jsonPath("$.durationMs").isEqualTo(1840);
  }

  @Test
  void anEmptyRunReportsZeros() {
    when(reconciliationService.runOnce()).thenReturn(Mono.just(new ReconciliationStats(0, 0, 0, 0,
        0, 320)));

    client.post().uri("/internal/reconciliation/run")
        .exchange()
        .expectStatus().isOk()
        .expectBody()
        .jsonPath("$.checked").isEqualTo(0)
        .jsonPath("$.durationMs").isEqualTo(320);
  }

  @Test
  void aRunWhileOneIsRunningIsAConflict() {
    when(reconciliationService.runOnce()).thenReturn(Mono.error(new ApiException(HttpStatus.CONFLICT,
        ApiException.Codes.RECONCILIATION_RUNNING, "A reconciliation pass is already running.")));

    client.post().uri("/internal/reconciliation/run")
        .exchange()
        .expectStatus().isEqualTo(409)
        .expectHeader().contentType(Problem.MEDIA_TYPE)
        .expectBody()
        .jsonPath("$.code").isEqualTo("RECONCILIATION_RUNNING")
        .jsonPath("$.instance").isEqualTo("/internal/reconciliation/run");
  }
}
