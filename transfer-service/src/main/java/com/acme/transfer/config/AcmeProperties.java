package com.acme.transfer.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * All infrastructure settings of the service.
 *
 * <p>Every value arrives as an environment variable (see {@code application.yml} for the mapping
 * and {@code docs/api.md} for the variable names) and has a default that works for local
 * development against a mock bank on {@code localhost:9090}. Nothing here is ever environment
 * specific: no hostnames or credentials beyond the local development defaults.
 */
@ConfigurationProperties(prefix = "acme")
public record AcmeProperties(
    String accountsUrl,
    String fxUrl,
    String fraudUrl,
    String coreUrl,
    int coreReadTimeoutMs,
    int coreMaxSessions,
    int coreBusyRetryAttempts,
    Duration coreBusyMinBackoff,
    Duration accountsTimeout,
    Duration fxTimeout,
    Duration fraudTimeout,
    Http http,
    Reconciliation reconciliation,
    FraudThreshold fraudThreshold,
    Batch batch,
    Sse sse) {

  /** Retry policy for outbound read calls (accounts and FX). */
  public record Http(int maxAttempts, Duration retryAfterDefault, Duration retryAfterMax,
                     Duration minBackoff) {
  }

  /** Reconciliation cadence and grace period. */
  public record Reconciliation(int intervalSeconds, int graceSeconds) {
  }

  /** Amounts at and above which a transfer is screened for fraud, per currency. */
  public record FraudThreshold(java.math.BigDecimal idr, java.math.BigDecimal usd,
                               java.math.BigDecimal sgd) {
  }

  /** Batch limits. */
  public record Batch(int maxLines, int accountConcurrency) {
  }

  /** Live events. */
  public record Sse(int heartbeatSeconds) {
  }
}
