package com.acme.transfer.support;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class MockBankContainerTest {

  @Container
  static final MockBankContainer MOCK_BANK = new MockBankContainer();

  @Test
  void resetsAndReportsStatistics() {
    MOCK_BANK.reset();
    MOCK_BANK.quiet();

    assertTrue(MOCK_BANK.stats().contains("\"core\""));
  }
}
