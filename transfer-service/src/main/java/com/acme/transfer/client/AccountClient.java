package com.acme.transfer.client;

import com.acme.transfer.config.AcmeProperties;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

/**
 * The Accounts service.
 *
 * <p>Reads are safe to repeat, so failures are retried by class: a throttling 429 waits for
 * {@code Retry-After}, a transient 503 backs off, and a 4xx is not retried. A 404 is an empty
 * result, not an error. Every call has a timeout, so a silent account service cannot stall a
 * transfer.
 */
@Component
public class AccountClient {

  private final WebClient webClient;
  private final AcmeProperties properties;

  public AccountClient(WebClient.Builder webClientBuilder, AcmeProperties properties) {
    this.webClient = webClientBuilder.baseUrl(properties.accountsUrl()).build();
    this.properties = properties;
  }

  /** Returns empty when the account does not exist. */
  public Mono<Account> getAccount(String accountNo) {
    return webClient.get()
        .uri("/accounts/{accountNo}", accountNo)
        .retrieve()
        .bodyToMono(Account.class)
        .timeout(properties.accountsTimeout())
        .retryWhen(HttpRetries.classified(properties.http().maxAttempts(),
            properties.http().retryAfterDefault(), properties.http().minBackoff(),
            properties.http().retryAfterMax()))
        .onErrorResume(WebClientResponseException.NotFound.class, notFound -> Mono.empty());
  }
}
