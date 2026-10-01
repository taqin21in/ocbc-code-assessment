package com.acme.transfer.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.acme.transfer.config.AcmeProperties;
import com.acme.transfer.dto.BatchLineResult;
import com.acme.transfer.dto.TransferRequest;
import com.acme.transfer.dto.TransferResource;
import com.acme.transfer.kit.VirtualThreads;
import com.acme.transfer.repository.TransferEntity;
import com.acme.transfer.service.TransferResult;
import com.acme.transfer.service.TransferService;
import com.acme.transfer.web.ApiException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.MediaType;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

/**
 * FEAT-201: the batch endpoint. The request is fed to the controller buffer by buffer, the way
 * Netty would, and the result stream is consumed line by line, so these tests exercise the
 * streaming pipeline itself rather than a buffered copy of it.
 */
class BatchControllerTest {

  private static final DataBufferFactory BUFFERS = DefaultDataBufferFactory.sharedInstance;
  private static final String ACCOUNT = "2000000001";
  private static final String OTHER_ACCOUNT = "2000000002";
  private static final String DESTINATION = "2000000099";

  private TransferService transferService;
  private ObjectMapper objectMapper;

  @BeforeEach
  void setUp() {
    transferService = mock(TransferService.class);
    objectMapper = new ObjectMapper();
    objectMapper.findAndRegisterModules();
  }

  private static AcmeProperties properties(int maxLines) {
    return new AcmeProperties(
        "http://accounts", "http://fx", "http://fraud", "http://core",
        2000, 10, 3, Duration.ofMillis(1), Duration.ofSeconds(5), Duration.ofSeconds(5),
        Duration.ofMillis(1500),
        new AcmeProperties.Http(4, Duration.ofSeconds(1), Duration.ofSeconds(30), Duration.ofMillis(100)),
        new AcmeProperties.Reconciliation(30, 10),
        new AcmeProperties.FraudThreshold(new BigDecimal("75000000.00"),
            new BigDecimal("5000.00"), new BigDecimal("6500.00")),
        new AcmeProperties.Batch(maxLines, 8),
        new AcmeProperties.Sse(15));
  }

  private static String line(String lineId, String key, String sourceAccount) {
    return "{\"lineId\":\"" + lineId + "\",\"idempotencyKey\":\"" + key + "\",\"sourceAccount\":\""
        + sourceAccount + "\",\"destinationAccount\":\"" + DESTINATION + "\","
        + "\"amount\":\"10.00\",\"currency\":\"USD\",\"description\":\"lunch\"}";
  }

  private static DataBuffer buffer(String text) {
    return BUFFERS.wrap(text.getBytes(StandardCharsets.UTF_8));
  }

  private MockServerWebExchange exchangeWith(String... chunks) {
    return MockServerWebExchange.from(MockServerHttpRequest
        .post("/api/v1/transfers/batch")
        .contentType(MediaType.parseMediaType("application/x-ndjson"))
        .body(Flux.fromArray(chunks).map(BatchControllerTest::buffer)));
  }

  private BatchController controller() {
    return controller(1000);
  }

  private BatchController controller(int maxLines) {
    return new BatchController(transferService, properties(maxLines), objectMapper);
  }

  private TransferResult completed() {
    TransferEntity entity = new TransferEntity("transfer-1", "COMPLETED", null, ACCOUNT,
        DESTINATION, new BigDecimal("10.00"), "USD", new BigDecimal("160000.00"), "IDR",
        new BigDecimal("16000.00"), "CT-1", "lunch", Instant.now(), Instant.now());
    return new TransferResult(201, TransferResource.from(entity), null, null);
  }

  private void stubSuccess() {
    when(transferService.execute(any(), any(TransferRequest.class)))
        .thenReturn(Mono.just(completed()));
  }

  private List<BatchLineResult> runAll(BatchController controller, MockServerWebExchange exchange) {
    return controller.batch(exchange).collectList().block();
  }

  @Test
  void everyValidLineBecomesAResultLine() {
    stubSuccess();

    List<BatchLineResult> results = runAll(controller(),
        exchangeWith(line("l1", "key-aaaa1111", ACCOUNT) + "\n",
            line("l2", "key-bbbb2222", OTHER_ACCOUNT) + "\n"));

    assertEquals(2, results.size());
    assertEquals("l1", results.get(0).lineId());
    assertEquals(201, results.get(0).httpStatus());
    assertEquals("COMPLETED", results.get(0).transfer().status());
    assertEquals("l2", results.get(1).lineId());
  }

  @Test
  void theBodyIsReadAcrossSeveralBuffersAsOneStream() {
    stubSuccess();

    // One line split over two buffers, and a second line arriving in the same chunk as the tail of
    // the first: the splitter must not lose, split or duplicate anything. The first buffer holds no
    // newline at all, so the partial line only completes when the rest of it arrives.
    String first = line("l1", "key-aaaa1111", ACCOUNT);
    String second = line("l2", "key-bbbb2222", OTHER_ACCOUNT);
    List<BatchLineResult> results = runAll(controller(),
        exchangeWith(first.substring(0, 40), first.substring(40) + "\n" + second + "\n"));

    assertEquals(List.of("l1", "l2"), results.stream().map(BatchLineResult::lineId).toList());
  }

  @Test
  void aBlankLineIsSkippedNotReported() {
    stubSuccess();

    List<BatchLineResult> results = runAll(controller(),
        exchangeWith(line("l1", "key-aaaa1111", ACCOUNT) + "\n\n",
            line("l2", "key-bbbb2222", OTHER_ACCOUNT) + "\r\n"));

    assertEquals(2, results.size());
  }

  @Test
  void aMalformedLineBecomesAnErrorAndTheBatchContinues() {
    stubSuccess();

    List<BatchLineResult> results = runAll(controller(),
        exchangeWith(line("l1", "key-aaaa1111", ACCOUNT) + "\n",
            "{this is not json\n",
            line("l3", "key-cccc3333", OTHER_ACCOUNT) + "\n"));

    assertEquals(3, results.size());
    assertEquals("l1", results.get(0).lineId());
    BatchLineResult broken = results.get(1);
    assertEquals(null, broken.lineId());
    assertEquals(400, broken.httpStatus());
    assertEquals("VALIDATION_ERROR", broken.error().code());
    assertEquals("l3", results.get(2).lineId());
  }

  @Test
  void aLineWithABadFieldIsRejectedWithoutCallingTheService() {
    stubSuccess();

    List<BatchLineResult> results = runAll(controller(),
        exchangeWith("{\"lineId\":\"l1\",\"idempotencyKey\":\"key-aaaa1111\","
            + "\"sourceAccount\":\"not-an-account\",\"destinationAccount\":\"" + DESTINATION + "\","
            + "\"amount\":\"10.00\",\"currency\":\"USD\"}\n"));

    assertEquals(1, results.size());
    assertEquals("l1", results.get(0).lineId());
    assertEquals(400, results.get(0).httpStatus());
    verify(transferService, never()).execute(any(), any());
  }

  @Test
  void aLineWithABadIdempotencyKeyIsRejected() {
    stubSuccess();

    List<BatchLineResult> results = runAll(controller(),
        exchangeWith("{\"lineId\":\"l1\",\"idempotencyKey\":\"short\","
            + "\"sourceAccount\":\"" + ACCOUNT + "\",\"destinationAccount\":\"" + DESTINATION
            + "\",\"amount\":\"10.00\",\"currency\":\"USD\"}\n"));

    assertEquals(1, results.size());
    assertEquals("VALIDATION_ERROR", results.get(0).error().code());
    verify(transferService, never()).execute(any(), any());
  }

  @Test
  void moreLinesThanTheMaximumReportsTheOverflow() {
    stubSuccess();

    List<BatchLineResult> results = runAll(controller(2),
        exchangeWith(line("l1", "key-aaaa1111", ACCOUNT) + "\n",
            line("l2", "key-bbbb2222", OTHER_ACCOUNT) + "\n",
            line("l3", "key-cccc3333", ACCOUNT) + "\n"));

    assertEquals(3, results.size());
    // The line past the limit was never parsed, so it has no lineId of its own.
    assertEquals(null, results.get(2).lineId());
    assertEquals("This batch has more than 2 lines.", results.get(2).error().message());
    verify(transferService, never()).execute(eq("key-cccc3333"), any());
  }

  @Test
  void theBodyIsNotReadBeyondTheLineLimit() {
    stubSuccess();

    // A client cannot stream an unbounded body through the endpoint: the limit stops the demand.
    List<BatchLineResult> results = runAll(controller(2),
        exchangeWith(line("l1", "key-aaaa1111", ACCOUNT) + "\n",
            line("l2", "key-bbbb2222", OTHER_ACCOUNT) + "\n",
            line("l3", "key-cccc3333", ACCOUNT) + "\n",
            line("l4", "key-dddd4444", OTHER_ACCOUNT) + "\n"));

    assertEquals(3, results.size());
    verify(transferService, never()).execute(eq("key-dddd4444"), any());
  }

  @Test
  void aFailedLineReportsItsCodeWithoutStoppingTheBatch() {
    when(transferService.execute(eq("key-aaaa1111"), any(TransferRequest.class)))
        .thenReturn(Mono.just(new TransferResult(409, null,
            ApiException.Codes.REQUEST_IN_PROGRESS, "A request with that key is in progress.")));
    when(transferService.execute(eq("key-bbbb2222"), any(TransferRequest.class)))
        .thenReturn(Mono.just(completed()));

    List<BatchLineResult> results = runAll(controller(),
        exchangeWith(line("l1", "key-aaaa1111", ACCOUNT) + "\n",
            line("l2", "key-bbbb2222", OTHER_ACCOUNT) + "\n"));

    assertEquals(409, results.get(0).httpStatus());
    assertEquals("REQUEST_IN_PROGRESS", results.get(0).error().code());
    assertEquals("l1", results.get(0).lineId());
    assertEquals(201, results.get(1).httpStatus());
  }

  @Test
  void anApiExceptionKeepsItsStatusAndCode() {
    when(transferService.execute(any(), any(TransferRequest.class)))
        .thenReturn(Mono.error(new ApiException(org.springframework.http.HttpStatus
            .UNPROCESSABLE_ENTITY, ApiException.Codes.IDEMPOTENCY_KEY_REUSED,
            "The key was used with a different body.")));

    List<BatchLineResult> results = runAll(controller(),
        exchangeWith(line("l1", "key-aaaa1111", ACCOUNT) + "\n"));

    assertEquals(422, results.get(0).httpStatus());
    assertEquals("IDEMPOTENCY_KEY_REUSED", results.get(0).error().code());
  }

  @Test
  void anUnexpectedFailureBecomesAnInternalErrorLine() {
    when(transferService.execute(any(), any(TransferRequest.class)))
        .thenReturn(Mono.error(new IllegalStateException("the pipeline broke")));

    List<BatchLineResult> results = runAll(controller(),
        exchangeWith(line("l1", "key-aaaa1111", ACCOUNT) + "\n"));

    assertEquals(500, results.get(0).httpStatus());
    assertEquals("INTERNAL_ERROR", results.get(0).error().code());
    assertFalse(results.get(0).error().message().contains("pipeline broke"),
        "the exception's message must not reach the client");
  }

  @Test
  void linesOfOneAccountRunInInputOrder() {
    AtomicReference<String> order = new AtomicReference<>("");
    when(transferService.execute(any(), any(TransferRequest.class))).thenAnswer(invocation ->
        Mono.fromCallable(() -> {
          String key = invocation.getArgument(0);
          order.updateAndGet(current -> current + key.charAt(4));
          return completed();
        }));

    runAll(controller(),
        exchangeWith(line("l1", "key-aaaa1111", ACCOUNT) + "\n",
            line("l2", "key-bbbb2222", ACCOUNT) + "\n",
            line("l3", "key-cccc3333", ACCOUNT) + "\n"));

    assertEquals("abc", order.get(), "the lines of one account ran out of input order");
  }

  @Test
  @Timeout(30)
  void linesOfDifferentAccountsRunConcurrently() {
    CountDownLatch otherLineStarted = new CountDownLatch(1);
    when(transferService.execute(eq("key-aaaa1111"), any(TransferRequest.class)))
        .thenReturn(Mono.defer(() -> {
          // This line cannot finish until the other account's line has started. If the endpoint
          // serialised the whole batch, the wait would time out and this line would fail instead of
          // completing.
          VirtualThreads.await(otherLineStarted);
          return Mono.just(completed());
        }).subscribeOn(Schedulers.boundedElastic()));
    when(transferService.execute(eq("key-bbbb2222"), any(TransferRequest.class)))
        .thenReturn(Mono.defer(() -> {
          otherLineStarted.countDown();
          return Mono.just(completed());
        }).subscribeOn(Schedulers.boundedElastic()));

    List<BatchLineResult> results = runAll(controller(),
        exchangeWith(line("l1", "key-aaaa1111", ACCOUNT) + "\n",
            line("l2", "key-bbbb2222", OTHER_ACCOUNT) + "\n"));

    assertEquals(2, results.size());
    results.forEach(result -> assertEquals(201, result.httpStatus(),
        () -> "line " + result.lineId() + " did not complete: " + result.error()));
  }

  @Test
  @Timeout(30)
  void resultsFollowCompletionOrder() {
    CountDownLatch firstMayFinish = new CountDownLatch(1);
    when(transferService.execute(eq("key-aaaa1111"), any(TransferRequest.class)))
        .thenReturn(Mono.defer(() -> {
          VirtualThreads.await(firstMayFinish);
          return Mono.just(completed());
        }).subscribeOn(Schedulers.boundedElastic()));
    when(transferService.execute(eq("key-bbbb2222"), any(TransferRequest.class)))
        .thenReturn(Mono.defer(() -> Mono.just(completed()))
            .subscribeOn(Schedulers.boundedElastic()));

    // Line 1 holds its result until line 2 has been served, so the response shows line 2 first:
    // completion order, not the order the lines arrived in.
    List<BatchLineResult> results = controller().batch(
            exchangeWith(line("l1", "key-aaaa1111", ACCOUNT) + "\n",
                line("l2", "key-bbbb2222", OTHER_ACCOUNT) + "\n"))
        .doOnNext(result -> {
          if ("l2".equals(result.lineId())) {
            firstMayFinish.countDown();
          }
        })
        .collectList()
        .block();

    assertEquals(List.of("l2", "l1"), results.stream().map(BatchLineResult::lineId).toList());
  }

  @Test
  @Timeout(30)
  void aClientThatLeavesStopsPullingLinesItHadNotStarted() {
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    when(transferService.execute(eq("key-aaaa1111"), any(TransferRequest.class)))
        .thenReturn(Mono.defer(() -> {
          started.countDown();
          VirtualThreads.await(release);
          return Mono.just(completed());
        }).subscribeOn(Schedulers.boundedElastic()));

    // The line in flight is allowed to finish; the two behind it are never pulled in, because a
    // gone client stops the demand the pipeline works on.
    StepVerifier.create(controller().batch(exchangeWith(
            line("l1", "key-aaaa1111", ACCOUNT) + "\n",
            line("l2", "key-bbbb2222", ACCOUNT) + "\n",
            line("l3", "key-cccc3333", ACCOUNT) + "\n")))
        .then(() -> VirtualThreads.await(started))
        .thenCancel()
        .verify();

    release.countDown();

    verify(transferService, never()).execute(eq("key-bbbb2222"), any());
    verify(transferService, never()).execute(eq("key-cccc3333"), any());
  }

  @Test
  void theStreamAnswersOneResultLinePerRequestLine() {
    stubSuccess();

    StepVerifier.create(controller().batch(exchangeWith(
            line("l1", "key-aaaa1111", ACCOUNT) + "\n",
            line("l2", "key-bbbb2222", OTHER_ACCOUNT) + "\n")))
        .assertNext(result -> assertEquals("l1", result.lineId()))
        .assertNext(result -> assertEquals("l2", result.lineId()))
        .verifyComplete();
  }
}
