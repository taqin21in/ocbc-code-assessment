package com.acme.transfer.client;

import com.acme.transfer.config.AcmeProperties;
import java.util.Map;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

/**
 * The Fraud service.
 *
 * <p>Two deliberate choices. First, no retry: a fraud failure is not something to wait out, because
 * the second choice is that a fraud failure is never an {@code ALLOW}. {@link FraudService} turns
 * an unreachable fraud service into {@code PENDING_REVIEW} with no posting (INC-104), so retrying
 * here would only delay that answer. Second, the fraud service's own documentation caps client
 * patience at 1.5 seconds: the timeout comes from configuration, never from a default that grows
 * by accident.
 */
@Component
public class FraudClient {

  private final WebClient webClient;
  private final AcmeProperties properties;

  public FraudClient(WebClient.Builder webClientBuilder, AcmeProperties properties) {
    this.webClient = webClientBuilder.baseUrl(properties.fraudUrl()).build();
    this.properties = properties;
  }

  public Mono<FraudDecision> assess(String transferId, String amount, String currency,
                                    String sourceAccount, String destinationAccount) {
    return webClient.post()
        .uri("/fraud/assessments")
        .bodyValue(Map.of(
            "transferId", transferId,
            "amount", amount,
            "currency", currency,
            "sourceAccount", sourceAccount,
            "destinationAccount", destinationAccount))
        .retrieve()
        .bodyToMono(FraudDecision.class)
        .timeout(properties.fraudTimeout());
  }
}
