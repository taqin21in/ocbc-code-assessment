package com.acme.transfer.config;

import java.util.List;
import java.util.UUID;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

/**
 * Gives every request a correlation ID.
 *
 * <p>The caller's {@code X-Correlation-Id} is preserved; a UUID is generated when it is absent.
 * The value is returned in the response header, kept on the exchange for the error handler, placed
 * in the Reactor context (so it reaches every thread the pipeline runs on, through
 * {@link ContextPropagation}) and put into MDC for the first log lines of the request.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class CorrelationIdFilter implements WebFilter {

  public static final String HEADER = "X-Correlation-Id";
  public static final String CONTEXT_KEY = "correlationId";
  public static final String EXCHANGE_ATTRIBUTE = CorrelationIdFilter.class.getName() + ".id";

  @Override
  public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
    List<String> sent = exchange.getRequest().getHeaders().get(HEADER);
    String correlationId = sent == null || sent.isEmpty() || sent.get(0) == null
        || sent.get(0).isBlank() ? UUID.randomUUID().toString() : sent.get(0).trim();

    exchange.getAttributes().put(EXCHANGE_ATTRIBUTE, correlationId);
    exchange.getResponse().getHeaders().add(HEADER, correlationId);

    MDC.put(ContextPropagation.MDC_KEY, correlationId);
    return chain.filter(exchange)
        .contextWrite(Context.of(CONTEXT_KEY, correlationId))
        .doFinally(signal -> MDC.remove(ContextPropagation.MDC_KEY));
  }
}
