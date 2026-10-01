package com.acme.transfer.controller;

import com.acme.transfer.config.AcmeProperties;
import com.acme.transfer.dto.BatchItem;
import com.acme.transfer.dto.BatchItem.Failure;
import com.acme.transfer.dto.BatchItem.Work;
import com.acme.transfer.dto.BatchLine;
import com.acme.transfer.dto.BatchLineResult;
import com.acme.transfer.dto.TransferRequest;
import com.acme.transfer.service.TransferService;
import com.acme.transfer.service.TransferRequests;
import com.acme.transfer.web.ApiException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * FEAT-201: batch transfers with streamed results.
 *
 * <h3>Streaming, end to end</h3>
 * The request body is read one buffer at a time and split into lines as they arrive; the response
 * is a {@link Flux} of result lines encoded as newline-delimited JSON. Nothing collects the whole
 * batch: not the request, not the results. The pipeline never sees the body, only the stream.
 *
 * <h3>One bad line never stops the batch</h3>
 * Parsing and validating happen per line and produce a result line for that line alone, so a
 * malformed or invalid line becomes an error result and the stream continues.
 *
 * <h3>Per-account order, cross-account concurrency</h3>
 * <pre>
 * Flux&lt;BatchItem&gt;
 *   → groupBy(sourceAccount)              lines of one account share a group
 *   → concatMap(runLine)                  inside a group: one at a time, in input order
 *   → flatMap(groups, accountConcurrency) across accounts: bounded concurrency
 * </pre>
 * A line for an account starts only after the previous line of the same account has a status;
 * lines of different accounts overlap. The accounts, the FX provider and the core still see only
 * as much concurrency as they accept — the core's ten sessions are the shared limit, because every
 * line goes through the same transfer pipeline as the single endpoint.
 *
 * <h3>Cancellation</h3>
 * Results are pulled by the response, so a client that disconnects stops the demand: lines that
 * were already in flight finish, and lines that had not started are never pulled in. A line that
 * completed after its client went away still has its idempotency key, so a replay finds the same
 * result.
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/transfers")
@RequiredArgsConstructor
public class BatchController {

  /** Every item that cannot run shares one group, so it goes straight to the response. */
  private static final String ERRORS_GROUP = "__errors__";

  private final TransferService transferService;
  private final AcmeProperties properties;
  private final ObjectMapper objectMapper;

  @PostMapping(
      value = "/batch",
      consumes = "application/x-ndjson",
      produces = "application/x-ndjson")
  public Flux<BatchLineResult> batch(ServerWebExchange exchange) {
    int maxLines = properties.batch().maxLines();
    return lines(exchange)
        .index((zeroBased, text) -> new LineInput(zeroBased.intValue() + 1, text))
        .take(maxLines + 1L)
        .concatMap(input -> input.number() > maxLines
            ? Flux.just(tooManyLines(input, maxLines))
            : Flux.just(parse(input)))
        .groupBy(BatchController::groupKey)
        .flatMap(group -> group.key().equals(ERRORS_GROUP)
                ? group.map(BatchController::toFailureResult)
                : group.ofType(Work.class).concatMap(this::runLine),
            properties.batch().accountConcurrency());
  }

  /** Splits the raw request body into lines, releasing every buffer it read. */
  private Flux<String> lines(ServerWebExchange exchange) {
    return Flux.defer(() -> {
      StringBuilder tail = new StringBuilder();
      return exchange.getRequest().getBody()
          .concatMapIterable(buffer -> {
            List<String> complete = new ArrayList<>();
            try {
              byte[] bytes = new byte[buffer.readableByteCount()];
              buffer.read(bytes);
              String chunk = new String(bytes, StandardCharsets.UTF_8);
              int start = 0;
              for (int i = 0; i < chunk.length(); i++) {
                char c = chunk.charAt(i);
                if (c == '\n') {
                  String line = tail + chunk.substring(start, i);
                  tail.setLength(0);
                  start = i + 1;
                  if (!line.isBlank()) {
                    complete.add(line.endsWith("\r") ? line.substring(0, line.length() - 1) : line);
                  }
                }
              }
              if (start < chunk.length()) {
                tail.append(chunk, start, chunk.length());
              }
            } finally {
              DataBufferUtils.release(buffer);
            }
            return complete;
          })
          .concatWith(Mono.defer(() -> tail.isEmpty()
              ? Mono.<String>empty()
              : Mono.just(tail.toString())));
    });
  }

  private static String groupKey(BatchItem item) {
    return item instanceof Work work ? work.sourceAccount() : ERRORS_GROUP;
  }

  private static BatchItem tooManyLines(LineInput input, int maxLines) {
    return new Failure(input.number(), null, ApiException.Codes.VALIDATION_ERROR,
        "This batch has more than " + maxLines + " lines.");
  }

  private BatchItem parse(LineInput input) {
    BatchLine parsed;
    try {
      parsed = objectMapper.readValue(input.text(), BatchLine.class);
    } catch (JsonProcessingException jsonError) {
      // A line that is not JSON cannot have a lineId, so its result carries the line number.
      return new Failure(input.number(), null, ApiException.Codes.VALIDATION_ERROR,
          "Line " + input.number() + " is not valid JSON.");
    } catch (RuntimeException runtimeError) {
      log.warn("Line {} could not be read", input.number(), runtimeError);
      return new Failure(input.number(), null, ApiException.Codes.VALIDATION_ERROR,
          "Line " + input.number() + " is not valid JSON.");
    }

    String keyProblem = TransferRequests.validateIdempotencyKey(parsed.idempotencyKey());
    if (keyProblem != null) {
      return new Failure(input.number(), parsed.lineId(), ApiException.Codes.VALIDATION_ERROR,
          keyProblem);
    }
    TransferRequest request = parsed.toRequest();
    String requestProblem = TransferRequests.validate(request);
    if (requestProblem != null) {
      return new Failure(input.number(), parsed.lineId(), ApiException.Codes.VALIDATION_ERROR,
          requestProblem);
    }
    return new Work(input.number(), parsed.lineId(), parsed.idempotencyKey(),
        parsed.sourceAccount(), request);
  }

  private Flux<BatchLineResult> runLine(Work work) {
    return transferService.execute(work.idempotencyKey(), work.request())
        .map(result -> result.transfer() != null
            ? BatchLineResult.ok(work.lineId(), result.httpStatus(), result.transfer())
            : BatchLineResult.failure(work.lineId(), work.lineNumber(), result.httpStatus(),
                result.code(), result.detail()))
        .onErrorResume(ApiException.class, api ->
            Mono.just(BatchLineResult.failure(work.lineId(), work.lineNumber(),
                api.getStatusCode(), api.getCode(), api.getMessage())))
        .onErrorResume(failure -> {
          log.error("Line {} of the batch failed unexpectedly", work.lineNumber(), failure);
          return Mono.just(BatchLineResult.failure(work.lineId(), work.lineNumber(), 500,
              ApiException.Codes.INTERNAL_ERROR, "An unexpected error occurred."));
        })
        .flux();
  }

  private static BatchLineResult toFailureResult(BatchItem item) {
    Failure failure = (Failure) item;
    return BatchLineResult.failure(failure.lineId(), failure.lineNumber(), 400, failure.code(),
        failure.message());
  }

  private record LineInput(int number, String text) {
  }
}
