package com.acme.transfer.client;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.math.BigDecimal;

/**
 * A fraud assessment decision.
 *
 * <p>{@code score} is a risk score, not a monetary amount: it is a {@link BigDecimal} anyway, so no
 * financial or near-financial value in the service is ever a {@code double}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record FraudDecision(String transferId, String decision, BigDecimal score) {
}
