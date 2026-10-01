package com.acme.transfer.service;

import com.acme.transfer.client.FraudClient;
import com.acme.transfer.client.FraudDecision;
import com.acme.transfer.config.AcmeProperties;
import com.acme.transfer.dto.TransferRequest;
import java.math.BigDecimal;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * The fraud screen, fail-closed.
 *
 * <p>The vendor code that was here did the opposite of what a bank needs: when the fraud service
 * was unavailable it logged a warning and returned {@code ALLOW}, so a large transfer went through
 * while fraud was down (INC-104). Now every failure — 5xx, timeout, connection refused, malformed
 * decision — becomes {@link FraudOutcome#UNAVAILABLE}, and {@link TransferService} turns that into
 * a transfer held for review with no core posting.
 *
 * <p>Transfers below the currency's threshold are not screened; the amount compared against the
 * threshold is a {@link BigDecimal}, never a {@code double} (INC-106).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FraudService {

  private final FraudClient fraudClient;
  private final AcmeProperties properties;

  public Mono<FraudOutcome> check(String transferId, TransferRequest request, BigDecimal amount) {
    if (amount.compareTo(threshold(request.currency())) < 0) {
      return Mono.just(FraudOutcome.ALLOW);
    }
    return fraudClient.assess(transferId, amount.toPlainString(), request.currency(),
            request.sourceAccount(), request.destinationAccount())
        .map(FraudDecision::decision)
        .map(this::toOutcome)
        .onErrorResume(failure -> {
          log.warn("Fraud check failed for transfer {}, holding for review", transferId, failure);
          return Mono.just(FraudOutcome.UNAVAILABLE);
        });
  }

  private FraudOutcome toOutcome(String decision) {
    return switch (decision) {
      case "ALLOW" -> FraudOutcome.ALLOW;
      case "DENY" -> FraudOutcome.DENY;
      case "REVIEW" -> FraudOutcome.REVIEW;
      // A decision outside the documented contract is not an allowance. The service answered, but
      // not with something we can act on, so a human decides instead of the money moving (INC-104).
      default -> FraudOutcome.REVIEW;
    };
  }

  private BigDecimal threshold(String currency) {
    return switch (currency) {
      case "IDR" -> properties.fraudThreshold().idr();
      case "SGD" -> properties.fraudThreshold().sgd();
      default -> properties.fraudThreshold().usd();
    };
  }
}
