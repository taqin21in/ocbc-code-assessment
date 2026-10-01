package com.acme.transfer.events;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

/**
 * FEAT-203: the live feed is hot, has no replay, and numbers each transfer's changes. A subscriber
 * that cannot keep up loses events instead of blocking the publisher.
 */
class TransferEventsTest {

  private TransferEvents events;

  @BeforeEach
  void setUp() {
    events = new TransferEvents(new SimpleMeterRegistry());
  }

  private static final String ACCOUNT = "2000000001";

  @Test
  void aSubscriberSeesEventsFromTheMomentItSubscribes() {
    events.publish("transfer-1", ACCOUNT, "PENDING", "CORE_TIMEOUT", Instant.now());

    StepVerifier.create(events.stream().take(2))
        .then(() -> {
          events.publish("transfer-2", ACCOUNT, "COMPLETED", null, Instant.now());
          events.publish("transfer-3", ACCOUNT, "REJECTED", "FRAUD_DENIED", Instant.now());
        })
        .assertNext(event -> assertEquals("transfer-2", event.transferId()))
        .assertNext(event -> assertEquals("transfer-3", event.transferId()))
        .verifyComplete();
  }

  @Test
  void anEventCarriesTheStatusAndItsReason() {
    StepVerifier.create(events.stream().take(1))
        .then(() -> events.publish("transfer-1", ACCOUNT, "PENDING_REVIEW", "FRAUD_UNAVAILABLE",
            Instant.now()))
        .assertNext(event -> {
          assertEquals("transfer-1", event.transferId());
          assertEquals("PENDING_REVIEW", event.status());
          assertEquals("FRAUD_UNAVAILABLE", event.reasonCode());
          assertEquals(ACCOUNT, event.sourceAccount());
        })
        .verifyComplete();
  }

  @Test
  void eventIdsAreNumberedPerTransfer() {
    StepVerifier.create(events.stream().take(3))
        .then(() -> {
          events.publish("transfer-1", ACCOUNT, "PENDING", "CORE_TIMEOUT", Instant.now());
          events.publish("transfer-2", ACCOUNT, "COMPLETED", null, Instant.now());
          events.publish("transfer-1", ACCOUNT, "COMPLETED", null, Instant.now());
        })
        .assertNext(first -> assertEquals(1, first.sequence()))
        .assertNext(second -> assertEquals(1, second.sequence()))
        .assertNext(third -> assertEquals(2, third.sequence()))
        .verifyComplete();
  }

  @Test
  void twoTransfersDoNotShareASequence() {
    StepVerifier.create(events.stream().take(2))
        .then(() -> {
          events.publish("transfer-1", ACCOUNT, "COMPLETED", null, Instant.now());
          events.publish("transfer-1", ACCOUNT, "REJECTED", "CORE_REJECTED", Instant.now());
        })
        .assertNext(first -> assertEquals(1, first.sequence()))
        .assertNext(second -> {
          assertEquals(2, second.sequence());
          assertEquals("transfer-1", second.transferId());
        })
        .verifyComplete();
  }

  @Test
  void theEventIdIsTheTransferAndItsSequence() {
    StepVerifier.create(events.stream().take(1))
        .then(() -> events.publish("transfer-1", ACCOUNT, "COMPLETED", null, Instant.now()))
        .assertNext(event -> assertEquals("transfer-1:1", event.transferId() + ":" + event.sequence()))
        .verifyComplete();
  }

  @Test
  void theSubscriberCountFollowsConnections() throws Exception {
    assertEquals(0, events.subscriberCount());

    CountDownLatch hold = new CountDownLatch(1);
    events.stream().subscribe(event -> {
      try {
        hold.await(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });

    CountDownLatch counted = new CountDownLatch(1);
    AtomicReference<Integer> seen = new AtomicReference<>();
    events.stream().take(1).subscribe(event -> {
      seen.set(events.subscriberCount());
      counted.countDown();
      hold.countDown();
    });

    events.publish("transfer-1", ACCOUNT, "COMPLETED", null, Instant.now());
    counted.await(5, TimeUnit.SECONDS);

    assertEquals(2, seen.get());
  }

  @Test
  void aPublisherIsNotBlockedByASlowSubscriber() {
    // A slow subscriber buffers nothing: the publisher returns immediately, and the event that the
    // subscriber was not ready for is simply absent from its stream.
    CountDownLatch slow = new CountDownLatch(1);
    events.stream().subscribe(event -> {
      try {
        slow.await(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });

    long startedAt = System.nanoTime();
    for (int i = 0; i < 100; i++) {
      events.publish("transfer-" + i, ACCOUNT, "COMPLETED", null, Instant.now());
    }
    long elapsed = System.nanoTime() - startedAt;

    org.junit.jupiter.api.Assertions.assertTrue(elapsed < 1_000_000_000L,
        "publishing took " + elapsed + " ns; a slow subscriber must not hold the publisher");
    slow.countDown();
  }

  @Test
  void publishingWithNoSubscribersIsNotAFailure() {
    events.publish("transfer-1", ACCOUNT, "COMPLETED", null, Instant.now());

    StepVerifier.create(events.stream().take(1))
        .then(() -> events.publish("transfer-2", ACCOUNT, "COMPLETED", null, Instant.now()))
        .assertNext(event -> assertEquals("transfer-2", event.transferId()))
        .verifyComplete();
  }
}
