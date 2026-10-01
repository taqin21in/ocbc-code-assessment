package com.acme.transfer.client;

import com.acme.transfer.config.AcmeProperties;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

/**
 * The FX service.
 *
 * <p>Rate reads are what INC-103 was about: a blanket {@code .retry(3)} retried every failure the
 * same way, ignored the provider's {@code Retry-After} on a 429 and retried 4xx failures that
 * cannot succeed. Failures are now classified (see {@link HttpRetries}): a throttling 429 waits
 * for the number of seconds the provider asks for, a transient 5xx backs off exponentially, and a
 * 4xx fails immediately. A missing {@code Retry-After} falls back to a documented default, so the
 * provider cannot stall us forever by omitting it.
 */
@Component
public class FxClient {

  private final WebClient webClient;
  private final AcmeProperties properties;

  public FxClient(WebClient.Builder webClientBuilder, AcmeProperties properties) {
    this.webClient = webClientBuilder.baseUrl(properties.fxUrl()).build();
    this.properties = properties;
  }

  public Mono<FxRate> getRate(String base, String quote) {
    return webClient.get()
        .uri(uri -> uri.path("/fx/rates")
            .queryParam("base", base).queryParam("quote", quote).build())
        .retrieve()
        .bodyToMono(FxRate.class)
        .timeout(properties.fxTimeout())
        .retryWhen(HttpRetries.classified(properties.http().maxAttempts(),
            properties.http().retryAfterDefault(), properties.http().minBackoff(),
            properties.http().retryAfterMax()));
  }
}
