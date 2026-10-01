package com.acme.core.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.ServerSocket;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CoreBankingClientTest {

  private StubCoreServer core;
  private CoreBankingClient client;

  private static final int READ_TIMEOUT_MS = 1000;

  private static final int SLOW_CORE_DELAY_MS = 1500;

  @BeforeEach
  void startCore() throws IOException {
    core = new StubCoreServer();
    client = CoreBankingClient.create(new CoreBankingConfig(core.baseUrl(), READ_TIMEOUT_MS));
  }

  @AfterEach
  void stopCore() {
    core.close();
  }

  @Test
  void postsTransfer() {
    PostingResult result = client.post(request("TRF-1001", "2000000001"));

    assertEquals("TRF-1001", result.reference());
    assertEquals(PostingResult.Status.POSTED, result.status());
    assertNotNull(result.coreTxnId());
    assertNull(result.reasonCode());
    assertNotNull(result.postedAt());
  }

  @Test
  void returnsRejectedPosting() {
    PostingResult result = client.post(request("TRF-1002", "2000000008"));

    assertEquals(PostingResult.Status.REJECTED, result.status());
    assertEquals("ACCOUNT_NOT_ACTIVE", result.reasonCode());
    assertNull(result.coreTxnId());
  }

  @Test
  void closesTheSessionAfterEachCall() {
    client.post(request("TRF-1003", "2000000001"));
    client.inquire("TRF-1003");

    assertEquals(2, core.sessionsOpened.get());
    assertEquals(2, core.sessionsClosed.get());
    assertEquals(0, core.openSessionCount());
  }

  @Test
  void busyCoreThrowsCoreBusyException() {
    core.busy = true;

    assertThrows(CoreBusyException.class, () -> client.post(request("TRF-1004", "2000000001")));
    assertEquals(0, core.sessionsOpened.get());
  }

  @Test
  void unreachableCoreThrowsCoreUnavailableException() throws IOException {
    CoreBankingClient offline =
        CoreBankingClient.create(new CoreBankingConfig("http://127.0.0.1:" + freePort(),
            READ_TIMEOUT_MS));

    assertThrows(CoreUnavailableException.class,
        () -> offline.post(request("TRF-1005", "2000000001")));
  }

  @Test
  void slowCoreThrowsCoreTimeoutException() {
    // The read timeout stays well under this delay, so the call is still the one that gives up.
    core.postingDelayMillis = SLOW_CORE_DELAY_MS;

    assertThrows(CoreTimeoutException.class, () -> client.post(request("TRF-1006", "2000000001")));
  }

  @Test
  void inquireFindsPosting() {
    client.post(request("TRF-2001", "2000000001"));

    Optional<PostingResult> found = client.inquire("TRF-2001");

    assertTrue(found.isPresent());
    assertEquals(PostingResult.Status.POSTED, found.get().status());
  }

  @Test
  void inquireReturnsEmptyForUnknownReference() {
    assertTrue(client.inquire("TRF-9999").isEmpty());
  }

  private static PostingRequest request(String reference, String debitAccount) {
    return new PostingRequest(reference, debitAccount, "1000000002",
        new BigDecimal("150.00"), "USD", new BigDecimal("2437575.56"), "IDR");
  }

  private static int freePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }
}
