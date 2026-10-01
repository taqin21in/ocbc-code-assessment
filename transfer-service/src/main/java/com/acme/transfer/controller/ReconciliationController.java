package com.acme.transfer.controller;

import com.acme.transfer.service.ReconciliationService;
import com.acme.transfer.service.ReconciliationStats;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * FEAT-202: the manual reconciliation trigger. Answers when the pass is done; 409
 * {@code RECONCILIATION_RUNNING} when one is already running (see {@link ReconciliationService}).
 */
@RestController
@RequestMapping("/internal/reconciliation")
@RequiredArgsConstructor
public class ReconciliationController {

  private final ReconciliationService reconciliationService;

  @PostMapping("/run")
  public Mono<ReconciliationStats> run() {
    return reconciliationService.runOnce();
  }
}
