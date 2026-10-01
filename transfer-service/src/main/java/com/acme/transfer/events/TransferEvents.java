package com.acme.transfer.events;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

/**
 * The live feed of transfer status changes.
 *
 * <h3>Backpressure: slow subscribers lose events, nobody is blocked</h3>
 *
 * The sink is a hot {@link Sinks.Many#multicast() multicast} with
 * {@link Sinks.EmitFailureHandler#busyLooping() direct best effort}: events are emitted to
 * subscribers that are ready, and a subscriber that is not ready simply misses the event. That is a
 * deliberate choice — the alternative is to buffer per subscriber, and a stalled client then holds
 * unbounded memory and eventually slows the transfers themselves. A subscriber that keeps up never
 * loses or duplicates an event.
 *
 * <h3>Producers</h3>
 *
 * Events arrive from request-handling threads (Netty event loop) and from the reconciliation
 * worker's virtual threads at the same time. The multicast sink serialises concurrent
 * {@code tryEmitNext} calls, so no lock is needed here; a failed attempt is a slow subscriber and
 * is logged, not propagated to the sender.
 *
 * <h3>Sequence</h3>
 *
 * Every transfer numbers its own changes from 1, so the SSE event id {@code transferId:sequence}
 * identifies a specific status of a specific transfer.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TransferEvents {

  /** The gauge the API contract asks for: {@code transfer.events.subscribers}. */
  private static final String SUBSCRIBERS_METRIC = "transfer.events.subscribers";

  private final MeterRegistry meterRegistry;
  private final Sinks.Many<TransferEvent> sink = Sinks.many().multicast().directBestEffort();
  private final Sinks.EmitFailureHandler onEmitFailure = (signal, failure) -> {
    if (failure == Sinks.EmitResult.FAIL_NON_SERIALIZED || failure == Sinks.EmitResult.FAIL_OVERFLOW
        || failure == Sinks.EmitResult.FAIL_TERMINATED || failure == Sinks.EmitResult.FAIL_CANCELLED) {
      // A slow or gone subscriber: drop the event for it, never block the sender.
      return true;
    }
    return false;
  };

  private final ConcurrentMap<String, AtomicInteger> sequences = new ConcurrentHashMap<>();
  private final AtomicInteger subscribers = new AtomicInteger();

  @PostConstruct
  void registerSubscriberGauge() {
    Gauge.builder(SUBSCRIBERS_METRIC, subscribers, AtomicInteger::get)
        .description("Subscribers of the transfer event stream")
        .register(meterRegistry);
  }

  /** Publishes one status change to every current subscriber. */
  public void publish(String transferId, String sourceAccount, String status, String reasonCode,
                      Instant updatedAt) {
    int sequence = sequences
        .computeIfAbsent(transferId, ignored -> new AtomicInteger()).incrementAndGet();
    TransferEvent event = new TransferEvent(transferId, sourceAccount, status, reasonCode,
        updatedAt, sequence);
    Sinks.EmitResult result = sink.tryEmitNext(event);
    if (result.isFailure()) {
      log.debug("Event {} for transfer {} was not delivered: {}", sequence, transferId, result);
    }
  }

  /**
   * The hot stream. No replay: a subscriber sees events from the moment it subscribes, nothing
   * earlier.
   *
   * <p>Delivery happens on a bounded-elastic worker of the subscriber's own, never on the thread
   * that publishes. Without that hand-off a slow subscriber would run its {@code onNext} on the
   * publishing thread — which is a Netty event loop during a transfer — and one stalled client
   * would stall money movement. This is what keeps that from happening.
   */
  public Flux<TransferEvent> stream() {
    return sink.asFlux()
        .publishOn(Schedulers.boundedElastic())
        .doOnSubscribe(subscription -> subscribers.incrementAndGet())
        .doFinally(signal -> {
          log.debug("An event subscriber left after {}", signal);
          subscribers.decrementAndGet();
        });
  }

  /** How many subscribers are connected right now, for tests. */
  public int subscriberCount() {
    return subscribers.get();
  }
}
