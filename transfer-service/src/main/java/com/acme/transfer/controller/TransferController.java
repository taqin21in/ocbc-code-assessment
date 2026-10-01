package com.acme.transfer.controller;

import com.acme.transfer.dto.TransferRequest;
import com.acme.transfer.dto.TransferResource;
import com.acme.transfer.repository.TransferEntity;
import com.acme.transfer.service.TransferResult;
import com.acme.transfer.service.TransferService;
import com.acme.transfer.web.ApiException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * The transfer API. Validation lives in {@code TransferRequests}, idempotency in
 * {@code IdempotencyService}, the pipeline in {@code TransferService}; this only wires an HTTP
 * exchange to them, so the batch endpoint can use exactly the same service without a second copy
 * of the business rules.
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/transfers")
@RequiredArgsConstructor
public class TransferController {

  private final TransferService transferService;

  @PostMapping
  public Mono<ResponseEntity<TransferResource>> createTransfer(
      @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
      @RequestBody(required = false) TransferRequest request) {
    log.info("Transfer request with Idempotency-Key {}", idempotencyKey);
    return transferService.execute(idempotencyKey, request)
        .flatMap(result -> result.transfer() != null
            ? Mono.just(ResponseEntity.status(result.httpStatus()).body(result.transfer()))
            : Mono.error(problem(result)));
  }

  @GetMapping("/{transferId}")
  public Mono<ResponseEntity<TransferResource>> getTransfer(@PathVariable String transferId) {
    return transferService.getTransfer(transferId)
        .map(entity -> ResponseEntity.ok().body(TransferResource.from(entity)))
        .switchIfEmpty(Mono.error(new ApiException(HttpStatus.valueOf(404),
            ApiException.Codes.TRANSFER_NOT_FOUND,
            "Transfer " + transferId + " was not found.")));
  }

  /** Validation and idempotency answers become an {@link ApiException}: the error handler renders it. */
  private static ApiException problem(TransferResult result) {
    return new ApiException(HttpStatus.valueOf(result.httpStatus()), result.code(),
        result.detail());
  }
}
