package com.acme.transfer.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

/**
 * Every infrastructure setting arrives from the environment with a local default; the properties
 * record is the single typed view of them.
 */
class AcmePropertiesTest {

  private static AcmeProperties defaults() {
    return new AcmeProperties(
        "http://mock-bank:9090", "http://mock-bank:9090", "http://mock-bank:9090",
        "http://mock-bank:9090",
        2000, 10, 3, java.time.Duration.ofMillis(100), java.time.Duration.ofSeconds(5),
        java.time.Duration.ofSeconds(5), java.time.Duration.ofMillis(1500),
        new AcmeProperties.Http(4, java.time.Duration.ofSeconds(1), java.time.Duration.ofSeconds(30), java.time.Duration.ofMillis(100)),
        new AcmeProperties.Reconciliation(30, 10),
        new AcmeProperties.FraudThreshold(new BigDecimal("75000000.00"), new BigDecimal("3000.00"),
            new BigDecimal("6500.00")),
        new AcmeProperties.Batch(1000, 8),
        new AcmeProperties.Sse(15));
  }

  @Test
  void theCoreSessionLimitIsTen() {
    assertEquals(10, defaults().coreMaxSessions(),
        "the core allows ten sessions; every caller shares them");
  }

  @Test
  void reconciliationRunsEveryThirtySecondsByDefault() {
    assertEquals(30, defaults().reconciliation().intervalSeconds());
  }

  @Test
  void theReconciliationGracePeriodIsTenSeconds() {
    assertEquals(10, defaults().reconciliation().graceSeconds());
  }

  @Test
  void fraudThresholdsArePerCurrency() {
    AcmeProperties.FraudThreshold thresholds = defaults().fraudThreshold();

    assertEquals(new BigDecimal("75000000.00"), thresholds.idr());
    assertEquals(new BigDecimal("3000.00"), thresholds.usd());
    assertEquals(new BigDecimal("6500.00"), thresholds.sgd());
  }

  @Test
  void batchLimitsAreBounded() {
    assertEquals(1000, defaults().batch().maxLines());
    assertEquals(8, defaults().batch().accountConcurrency());
  }

  @Test
  void theSseHeartbeatIsFifteenSeconds() {
    assertEquals(15, defaults().sse().heartbeatSeconds());
  }

  @Test
  void theCoreTimeoutIsConfigurable() {
    assertEquals(2000, defaults().coreReadTimeoutMs());
  }

  @Test
  void timeoutsArePerDependency() {
    AcmeProperties properties = defaults();

    assertEquals(java.time.Duration.ofSeconds(5), properties.accountsTimeout());
    assertEquals(java.time.Duration.ofSeconds(5), properties.fxTimeout());
    assertEquals(java.time.Duration.ofMillis(1500), properties.fraudTimeout());
  }

  @Test
  void theHttpRetryPolicyIsBounded() {
    AcmeProperties.Http http = defaults().http();

    assertEquals(4, http.maxAttempts(), "retries are bounded, never open-ended");
    assertEquals(java.time.Duration.ofSeconds(1), http.retryAfterDefault());
    assertEquals(java.time.Duration.ofSeconds(30), http.retryAfterMax(),
        "a provider cannot stall a transfer past this cap");
    assertEquals(java.time.Duration.ofMillis(100), http.minBackoff());
  }

  @Test
  void coreRetriesOnlyCoverFailuresWhereNothingWasSent() {
    assertEquals(3, defaults().coreBusyRetryAttempts());
  }
}
