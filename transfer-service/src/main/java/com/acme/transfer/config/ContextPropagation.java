package com.acme.transfer.config;

import jakarta.annotation.PostConstruct;
import org.slf4j.MDC;
import org.springframework.context.annotation.Configuration;
import io.micrometer.context.ContextRegistry;
import reactor.core.publisher.Hooks;

/**
 * Carries the correlation ID from the Reactor context into MDC on every thread a request runs on:
 * the Netty event loop, virtual threads started through the Reactor schedulers, and the
 * reconciliation worker. With automatic context propagation enabled, Micrometer restores the
 * ThreadLocal around each task, so every log line written for the request sees it.
 */
@Configuration
public class ContextPropagation {

  public static final String MDC_KEY = "correlationId";

  @PostConstruct
  void enable() {
    ContextRegistry.getInstance()
        .registerThreadLocalAccessor(MDC_KEY,
            () -> MDC.get(MDC_KEY),
            value -> MDC.put(MDC_KEY, value),
            () -> MDC.remove(MDC_KEY));
    Hooks.enableAutomaticContextPropagation();
  }
}
