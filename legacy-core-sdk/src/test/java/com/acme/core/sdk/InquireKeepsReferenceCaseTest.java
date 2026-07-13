package com.acme.core.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** SDK-101: inquire() upper-cased the reference, so lower-case references were never found. */
class InquireKeepsReferenceCaseTest {

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
  void findsPostingWithLowerCaseReference() {
    String reference = "3f2b8c1e-7d4a-4b9e-9c1a-5e6f7a8b9c0d";
    client.post(new PostingRequest(reference, "2000000001", "1000000002",
        new BigDecimal("150.00"), "USD", new BigDecimal("2437575.56"), "IDR"));

    Optional<PostingResult> found = client.inquire(reference);

    assertTrue(found.isPresent());
    assertEquals(reference, found.get().reference());
  }
}
