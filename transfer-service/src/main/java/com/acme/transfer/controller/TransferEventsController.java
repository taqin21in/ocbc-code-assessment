package com.acme.transfer.controller;

import com.acme.transfer.config.AcmeProperties;
import com.acme.transfer.events.TransferEvent;
import com.acme.transfer.events.TransferEvents;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

/**
 * FEAT-203: live transfer events.
 *
 * <p>Hot stream: a subscriber sees events from the moment it connects, nothing earlier. Every event
 * is of type {@code transfer-status}, carries the transfer's current status and the change's
 * sequence number in its id, and reaches the subscriber as JSON. A comment heartbeat every
 * {@code sse.heartbeat-seconds} keeps the connection alive without sending an event.
 *
 * <p>The backpressure strategy is documented in {@link TransferEvents}: a subscriber that cannot
 * keep up loses its oldest events and is never allowed to slow a transfer, a reconciliation pass
 * or another subscriber. A disconnect drops everything the subscriber held, because nothing is
 * buffered for it.
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/transfers")
@RequiredArgsConstructor
public class TransferEventsController {

  private final TransferEvents events;
  private final AcmeProperties properties;
  private final ObjectMapper objectMapper;

  @GetMapping(value = "/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  public Flux<ServerSentEvent<String>> events(
      @RequestParam(name = "sourceAccount", required = false) String sourceAccount) {
    Duration heartbeat = Duration.ofSeconds(properties.sse().heartbeatSeconds());

    Flux<ServerSentEvent<String>> heartbeats = Flux.interval(heartbeat)
        .map(tick -> ServerSentEvent.<String>builder().comment("heartbeat").build());

    Flux<ServerSentEvent<String>> statuses = events.stream()
        .filter(event -> sourceAccount == null || sourceAccount.equals(event.sourceAccount()))
        .concatMap(this::toServerSentEvent);

    return Flux.merge(statuses, heartbeats);
  }

  private org.reactivestreams.Publisher<ServerSentEvent<String>> toServerSentEvent(
      TransferEvent event) {
    String body;
    try {
      // Only the four fields the contract specifies: never the internal sourceAccount.
      body = objectMapper.writeValueAsString(new EventPayload(event.transferId(), event.status(),
          event.reasonCode(), event.updatedAt()));
    } catch (JsonProcessingException jsonError) {
      log.error("Could not serialize event {} for transfer {}", event.sequence(),
          event.transferId(), jsonError);
      return Flux.empty();
    }
    return Flux.just(ServerSentEvent.<String>builder()
        .event("transfer-status")
        .id(event.transferId() + ":" + event.sequence())
        .data(body)
        .build());
  }

  /** The SSE payload, exactly as {@code docs/api.md} specifies it. */
  private record EventPayload(String transferId, String status, String reasonCode,
                              java.time.Instant updatedAt) {
  }
}
