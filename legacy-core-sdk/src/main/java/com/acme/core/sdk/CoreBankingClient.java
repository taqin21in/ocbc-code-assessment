package com.acme.core.sdk;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Objects;
import java.util.Optional;

/**
 * Blocking client for the ACME core banking system.
 *
 * <p>Every call opens a core session, performs one operation and closes the session. Session I/O
 * is synchronized. Instances are thread-safe: create one per application and share it.
 *
 * <p>The core allows at most 10 open sessions at a time. When none is free, calls fail with
 * {@link CoreBusyException}.
 */
public final class CoreBankingClient {

  private final String baseUrl;
  private final SdkTimeouts timeouts;
  private final HttpTransport transport;

  private CoreBankingClient(CoreBankingConfig config) {
    this.baseUrl = config.baseUrl();
    this.timeouts = SdkTimeouts.from(config);
    this.transport = new HttpTransport(baseUrl, timeouts);
  }

  /** Creates a client. Nothing is sent to the core until the first call. */
  public static CoreBankingClient create(CoreBankingConfig config) {
    Objects.requireNonNull(config, "config");
    return new CoreBankingClient(config);
  }

  /**
   * Posts a transfer to the core.
   *
   * <p><b>Not idempotent.</b> Every call creates a new posting, even when the core has already
   * seen the reference.
   *
   * @return the posting, with status {@code POSTED} or {@code REJECTED}
   * @throws CoreBusyException no session was available; nothing was sent
   * @throws CoreUnavailableException the core could not be reached; nothing was sent
   * @throws CoreTimeoutException the request was sent but no response arrived in time; the
   *     outcome is unknown, use {@link #inquire(String)}
   * @throws CoreBankingException the core answered with an unexpected response
   */
  public PostingResult post(PostingRequest request) {
    Objects.requireNonNull(request, "request");
    return withSession("POST /core/postings",
        session -> transport.createPosting(session.id(), request));
  }

  /**
   * Looks up a posting by the reference it was posted with. Read-only and safe to retry.
   *
   * @return the posting, or empty when the core has no posting with this reference
   * @throws CoreBusyException no session was available
   * @throws CoreUnavailableException the core could not be reached
   * @throws CoreTimeoutException no response arrived in time
   */
  public Optional<PostingResult> inquire(String reference) {
    Objects.requireNonNull(reference, "reference");
    return withSession("GET /core/postings",
        session -> transport.findPosting(session.id(), reference));
  }

  private <T> T withSession(String operation, SessionCall<T> call) {
    Session session = new Session(transport);
    synchronized (session) {
      session.open();
      try {
        T result = call.execute(session);
        session.close();
        return result;
      } catch (SocketTimeoutException e) {
        throw new CoreTimeoutException(
            operation + ": no response from " + baseUrl + " within "
                + timeouts.readMillis() + " ms", e);
      } catch (IOException e) {
        session.close();
        throw new CoreTimeoutException(operation + ": connection lost, outcome unknown", e);
      } catch (RuntimeException e) {
        session.close();
        throw e;
      }
    }
  }

  @FunctionalInterface
  private interface SessionCall<T> {
    T execute(Session session) throws IOException;
  }
}
