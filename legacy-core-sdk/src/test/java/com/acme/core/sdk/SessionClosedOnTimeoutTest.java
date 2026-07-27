package com.acme.core.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.math.BigDecimal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** SDK-107: a read timeout left the session open until the core expired it. */
class SessionClosedOnTimeoutTest {

  private StubCoreServer core;
  private CoreBankingClient client;

  @BeforeEach
  void startCore() throws IOException {
    core = new StubCoreServer();
    client = CoreBankingClient.create(new CoreBankingConfig(core.baseUrl(), 300));
  }

  @AfterEach
  void stopCore() {
    core.close();
  }

  @Test
  void closesTheSessionWhenTheCoreDoesNotAnswerInTime() {
    core.postingDelayMillis = 1500;

    CoreTimeoutException timeout = assertThrows(CoreTimeoutException.class,
        () -> client.post(new PostingRequest("TRF-3001", "2000000001", "1000000002",
            new BigDecimal("150.00"), "USD", new BigDecimal("2437575.56"), "IDR")));

    assertEquals(1, core.sessionsOpened.get());
    assertEquals(1, core.sessionsClosed.get());
    assertEquals(0, core.openSessionCount());
    assertEquals("POST /core/postings: no response from " + core.baseUrl() + " within 300 ms",
        timeout.getMessage());
  }
}
