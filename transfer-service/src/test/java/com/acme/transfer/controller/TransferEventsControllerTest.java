package com.acme.transfer.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.acme.transfer.config.AcmeProperties;
import com.acme.transfer.events.TransferEvents;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

/**
 * FEAT-203: the live feed at the HTTP boundary. The stream is hot, carries one event type, ids a
 * status change as {@code transferId:sequence}, keeps itself alive with a comment heartbeat, and
 * never replays anything a subscriber missed.
 */
class TransferEventsControllerTest {

  private static final String ACCOUNT = "2000000001";
  private static final String OTHER_ACCOUNT = "2000000002";

  private TransferEvents events;
  private ObjectMapper objectMapper;
  private TransferEventsController controller;

  @BeforeEach
  void setUp() {
    events = new TransferEvents(new SimpleMeterRegistry());
    objectMapper = new ObjectMapper();
    objectMapper.findAndRegisterModules();
    controller = new TransferEventsController(events, properties(15), objectMapper);
  }

  private static AcmeProperties properties(int heartbeatSeconds) {
    return new AcmeProperties(
        "http://accounts", "http://fx", "http://fraud", "http://core",
        2000, 10, 3, Duration.ofMillis(1), Duration.ofSeconds(5), Duration.ofSeconds(5),
        Duration.ofMillis(1500),
        new AcmeProperties.Http(4, Duration.ofSeconds(1), Duration.ofSeconds(30), Duration.ofMillis(100)),
        new AcmeProperties.Reconciliation(30, 10),
        new AcmeProperties.FraudThreshold(new BigDecimal("75000000.00"),
            new BigDecimal("5000.00"), new BigDecimal("6500.00")),
        new AcmeProperties.Batch(1000, 8),
        new AcmeProperties.Sse(heartbeatSeconds));
  }

  private void publish(String transferId, String account, String status) {
    events.publish(transferId, account, status, null, Instant.now());
  }

  @Test
  void anEventCarriesOnlyTheFourContractFields() throws Exception {
    StepVerifier.create(controller.events(null).take(1))
        .then(() -> publish("transfer-2", ACCOUNT, "PENDING"))
        .assertNext(sse -> {
          assertEquals("transfer-2:1", sse.id());
          Map<String, Object> payload = readJson(sse.data());
          assertEquals(4, payload.size(), payload::toString);
          assertEquals("transfer-2", payload.get("transferId"));
          assertEquals("PENDING", payload.get("status"));
        })
        .verifyComplete();
  }

  private Map<String, Object> readJson(String data) {
    try {
      return objectMapper.readValue(data, new TypeReference<>() {
      });
    } catch (JsonProcessingException jsonError) {
      throw new AssertionError("could not parse " + data, jsonError);
    }
  }

  @Test
  void theSourceAccountNeverLeavesTheServer() {
    StepVerifier.create(controller.events(null).take(1))
        .then(() -> publish("transfer-3", ACCOUNT, "COMPLETED"))
        .assertNext(sse -> assertTrue(!sse.data().contains(ACCOUNT), sse.data()))
        .verifyComplete();
  }

  @Test
  void theStreamReplaysNothing() {
    publish("transfer-early", ACCOUNT, "COMPLETED");

    // A subscriber that arrives later sees only what happens after it subscribed.
    StepVerifier.create(controller.events(null).take(1))
        .then(() -> publish("transfer-late", ACCOUNT, "COMPLETED"))
        .assertNext(sse -> assertEquals("transfer-late:1", sse.id()))
        .verifyComplete();
  }

  @Test
  void theStreamCanBeFilteredToOneAccount() {
    StepVerifier.create(controller.events(ACCOUNT).take(1))
        .then(() -> {
          publish("transfer-other", OTHER_ACCOUNT, "COMPLETED");
          publish("transfer-mine", ACCOUNT, "COMPLETED");
        })
        .assertNext(sse -> assertEquals("transfer-mine:1", sse.id()))
        .verifyComplete();
  }

  @Test
  void aSubscriberWithoutAFilterSeesEveryAccount() {
    StepVerifier.create(controller.events(null).take(2))
        .then(() -> {
          publish("transfer-a", ACCOUNT, "COMPLETED");
          publish("transfer-b", OTHER_ACCOUNT, "COMPLETED");
        })
        .assertNext(sse -> assertEquals("transfer-a:1", sse.id()))
        .assertNext(sse -> assertEquals("transfer-b:1", sse.id()))
        .verifyComplete();
  }

  @Test
  void aHeartbeatKeepsTheConnectionAliveWithoutAnEvent() {
    TransferEventsController fastHeartbeat = new TransferEventsController(events, properties(1),
        objectMapper);

    StepVerifier.withVirtualTime(() -> fastHeartbeat.events(null))
        .thenAwait(Duration.ofSeconds(3))
        .expectNextMatches(sse -> sse.comment() != null && sse.data() == null)
        .expectNextMatches(sse -> sse.comment() != null)
        .thenCancel()
        .verify();
  }

  @Test
  void anEventStillReachesASubscriberAlongsideTheHeartbeats() {
    TransferEventsController fastHeartbeat = new TransferEventsController(events, properties(1),
        objectMapper);

    StepVerifier.withVirtualTime(() -> fastHeartbeat.events(null))
        .then(() -> publish("transfer-4", ACCOUNT, "COMPLETED"))
        .expectNextMatches(sse -> "transfer-4:1".equals(sse.id()))
        .thenAwait(Duration.ofSeconds(1))
        .expectNextMatches(sse -> sse.comment() != null)
        .thenCancel()
        .verify();
  }
}
