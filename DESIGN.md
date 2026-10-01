# Design

Answers to every section of `docs/DESIGN_TEMPLATE.md`, referring to this repository's own code and
runs. Short and specific beats long and general.

## 1. Problems found

Beyond the incident list, the original code had these defects. Each row names the file, the cause,
the fix and the test that proves it.

| Problem | File / cause | Fix | Test |
|---|---|---|---|
| The whole transfer pipeline was retried, core post included | `TransferService` — `.retry(3)` wrapped the entire flow; a core timeout after the posting applied it was retried, debiting the customer twice (INC-101) and reporting FAILED though the money had moved (INC-109) | A core timeout is terminal for the request path: the transfer is stored `PENDING` / `CORE_TIMEOUT` (HTTP 202). Only `CoreBusyException` and `CoreUnavailableException` — where the SDK documents that nothing was sent — are retried, and only inside `DefaultCoreGateway` | `TransferServiceTest.coreTimeoutBecomesPendingAndIsNeverReposted` (asserts one post, 202, `PENDING`, `CORE_TIMEOUT`); reproduced end to end in §3 below |
| Core calls ran on the event loop | The vendor called the blocking SDK directly from the reactive pipeline (INC-102) | `DefaultCoreGateway.withPermit` runs every SDK call as `Mono.fromCallable(...).subscribeOn(virtualThreadExecutor)`; `VirtualThreads` owns the one virtual-thread executor | `DefaultCoreGatewayTest.aSlowCoreCallDoesNotBlockTheSubscriberThread` |
| No global session limit | Each caller path could open its own sessions; reconciliation flooded core with "session busy" (INC-108) | One fair `Semaphore(10)` in `DefaultCoreGateway`, shared by the API, batch and reconciliation | `DefaultCoreGatewayTest.twoHundredCallersNeverExceedTenInFlight` |
| FX retried everything three times | `FxClient` used `.retry(3)` regardless of status, ignoring `Retry-After` (INC-103) | `HttpRetries.classified`: 429 waits `Retry-After` (capped), 5xx bounded exponential backoff, other 4xx not retried | `HttpRetriesTest` (virtual time: 429 honours the header, 500 backs off, 404 errors immediately, attempts bounded) |
| Fraud failure became `ALLOW` | The vendor caught fraud errors, logged a warning and returned `ALLOW` (INC-104) | `FraudService` maps every failure to `FraudOutcome.UNAVAILABLE` → `PENDING_REVIEW` / `FRAUD_UNAVAILABLE`, no post | `TransferServiceTest.fraudUnavailableHoldsTheTransferAndPostsNothing` |
| Fraud decisions outside the contract released money | `FraudService.toOutcome` mapped any unknown decision to `ALLOW` — fail-open on a contract the fraud API does not define | An unrecognized decision becomes `REVIEW`: a human decides, no money moves | `FraudServiceTest.anUnknownDecisionIsHeldForReview`; `TransferServiceTest.fraudReviewHoldsTheTransferAndPostsNothing` |
| Idempotency was SELECT-then-INSERT | Two concurrent retries both saw "missing" and both inserted (INC-105); the fingerprint was `hashCode()` | `IdempotencyService.begin` inserts the placeholder first; the V3 primary key makes exactly one insert win. Fingerprint is SHA-256 of canonical JSON (`RequestFingerprint`) | `IdempotencyServiceTest`; `V3__idempotency_constraints.sql` adds the primary key |
| Credit amounts off by 0.01 | `FxService` computed `Math.round(amount.doubleValue() * rate.doubleValue() * 100) / 100.0` (INC-106) | `amount.multiply(rate).setScale(2, RoundingMode.HALF_EVEN)` in `BigDecimal` end to end | `MoneyTest` rounding edges; `FxServiceTest` |
| Reconciliation re-POSTed timed-out transfers | `ReconciliationJob` ran on a fixed 50-thread pool and re-posted | Replaced by `ReconciliationService`: inquire-only, virtual threads, one pass at a time | `ReconciliationServiceTest` decision matrix |
| Errors were unstructured | The web client showed "Unrecognised error response", sometimes with a stack trace (INC-111) | `ProblemWebExceptionHandler` renders RFC 9457 `application/problem+json` with `code` and `correlationId`, never a stack or class name | `ProblemWebExceptionHandlerTest` |
| No correlation ID | Support could not find a request in the logs (INC-111) | `CorrelationIdFilter` preserves or generates `X-Correlation-Id`, returns it, writes it to the Reactor context; `ContextPropagation` carries it into MDC on every thread | `CorrelationIdFilterTest`; the trace in §3 |
| `Retry-After` could stall a transfer indefinitely | `HttpRetries` used the header verbatim with no ceiling | Added `http.retry-after-max` (default 30 s); the wait is `min(header, cap)` | `HttpRetriesTest.aHostileRetryAfterIsCapped` |
| Container OOMKilled in UAT | `-Xmx1g` against a 512 MB limit (INC-107) | Multi-stage Dockerfile, `MaxRAMPercentage`, non-root JRE image | §10 below |
| Services unreachable in compose | `localhost` inside containers (INC-110) | Service-name DNS and environment variables in `docker-compose.yml` | Verified by running the stack; see §10 |

## 2. Threads

Three kinds of thread do the work.

**Netty event loop** (`reactor-http-epoll-*`, `reactor-tcp-epoll-*`). Controllers, the WebClient calls
to accounts/FX/fraud, R2DBC, and the SSE outbound all run here. Nothing blocks: no `.block()`, no
blocking I/O, no `Thread.sleep` in any code that runs on these threads. The only scheduler the event
loop hands off to is `boundedElastic`, used once, in `TransferEvents.stream():94`, so a subscriber's
`onNext` never runs on the publishing thread.

**Virtual threads** (`virtual-*`). One executor, `VirtualThreads.virtualThreadExecutor()`, a
`newVirtualThreadPerTaskExecutor()`. Two consumers:
`DefaultCoreGateway.withPermit` (every core SDK call) and `ReconciliationService` (the pass and one
virtual thread per transfer it inquires about). These threads may block — that is their job.

**The `boundedElastic` worker.** SSE subscriber delivery only.

Where blocking happens, deliberately and only here:

* `CoreBankingClient.post` / `inquire` — synchronized session I/O over `HttpURLConnection`, on a
  virtual thread inside `DefaultCoreGateway`.
* `ReconciliationService.reconcile:115,179,218` — `.block()` on the reactive repository and the
  gateway, on a virtual thread. The contract sanctions exactly this ("The worker reads PENDING
  transfers from the reactive `TransferRepository` and blocks on it from its virtual threads").

Evidence from a real run: the trace in §3 shows the core post running on `virtual-45` while the
request lines before it run on event-loop threads.

## 3. Trace

One transfer that became `PENDING`, from the containerized run described in §10, with the caller's
correlation ID `trace-pending-001` on every line. The mock bank was set to hang the core for 5 s and
apply the posting anyway — the INC-101 condition.

```text
19:52:28.800 INFO [trace-pending-001] [or-http-epoll-4] TransferController   : Transfer request with Idempotency-Key pending-repro-001
19:52:29.516 INFO [trace-pending-001] [tor-tcp-epoll-2]  TransferService     : Creating transfer 325ebaaa-…: 250.00 USD from 2000000001 to 1000000002
19:52:31.841 INFO [trace-pending-001] [or-http-epoll-2]  TransferService     : Posting transfer 325ebaaa-… through core banking
19:52:34.704 WARN [trace-pending-001] [     virtual-45] TransferService     : Core banking did not answer for transfer 325ebaaa-…; leaving it PENDING for reconciliation
19:52:34.753 INFO [trace-pending-001] [     virtual-45] TransferService     : Transfer 325ebaaa-… is PENDING CORE_TIMEOUT
19:52:50.329 INFO [-               ] [     virtual-52] ReconciliationService: Transfer 325ebaaa-… is POSTED in core banking: COMPLETED
```

The API answered `202` with `status=PENDING, reasonCode=CORE_TIMEOUT`. The mock bank's
`/__admin/stats` showed exactly **one** posting for that reference. Fifteen seconds later the
scheduled pass inquired, found it `POSTED`, and moved the transfer to `COMPLETED` with
`coreTxnId=CT00000001`. No second posting ever occurred.

Note the thread column: the request enters on event-loop threads, the core call runs on a virtual
thread, the reconciliation pass runs on another. The scheduled pass has no request correlation ID,
so its line carries the pass-scoped `reconciliation-N` MDC instead.

## 4. Core SDK

All SDK calls go through the single `DefaultCoreGateway` bean. Concurrency is limited by one fair
`Semaphore(properties.coreMaxSessions())` — default 10, `application.yml:40` — created once in the
gateway's constructor. The transfer API, the batch endpoint and reconciliation all reach the core
through that one bean, so they share the same ten permits. Nothing else in the codebase creates a
semaphore (`grep -rn Semaphore transfer-service/src/main` returns the gateway alone).

When 200 requests arrive at once: 200 virtual threads are cheap, so all 200 pipelines proceed up to
the post; 10 acquire a permit and open a session; 190 park on `acquireUninterruptibly()`. The core
sees 10 open sessions, never 11. As each call finishes, `finally` releases its permit and the next
caller takes it. Verified by `DefaultCoreGatewayTest.twoHundredCallersNeverExceedTenInFlight`, which
fires 200 concurrent posts behind a counting barrier and asserts the high-water mark is exactly 10.

`acquireUninterruptibly` rather than `acquire` keeps permit accounting correct when a carrier thread
is interrupted; `release` in `finally` keeps it correct on timeout. Both are covered by
`DefaultCoreGatewayTest.aPermitIsReleasedAfterATimeout`.

## 5. Timeouts and retries

Per dependency, with defaults from `application.yml`:

| Dependency | Timeout | Retries | Backoff | Retried statuses | Why |
|---|---|---|---|---|---|
| Accounts | 5 s per attempt | 4 attempts total | 429: `Retry-After` (capped 30 s); 5xx: 100 ms × 2^n | 429, 5xx, read timeout | Read-only; repeating it is always safe |
| FX | 5 s per attempt | 4 attempts total | as accounts | 429, 5xx, read timeout | INC-103: `Retry-After` honoured, 4xx never retried |
| Fraud | 1.5 s | none | — | none | The fraud API caps client patience at 1.5 s, and retrying only delays the fail-closed answer |
| Core (post) | 2 s read | 3 attempts | 100 ms × 2^n | `CoreBusyException`, `CoreUnavailableException` only | Those mean nothing was sent. A timeout means the opposite |
| Core (inquire) | 2 s read | 3 attempts | as post | as post | Read-only and safe to repeat |

Worst case for one transfer, serially: accounts 4 × 5 s + backoff ≈ 20.7 s (the two account reads
run in parallel, so the pair costs the worse one); FX the same ≈ 20.7 s; fraud 1.5 s; core
4 × 2 s + backoff ≈ 8.7 s. Total ≈ **51.6 s** plus database round trips. Two amplifiers: a large
`Retry-After` waits up to the 30 s cap per retry, and a core with all ten sessions busy queues
callers on the semaphore, each then paying its own core budget. No retry applies to a core timeout,
which is the property INC-101 depends on.

## 6. Fraud unavailable

`PENDING_REVIEW` with `FRAUD_UNAVAILABLE`, HTTP 202. Chosen over `FAILED` because a fraud outage is
a decision we have not made, not a decision to refuse: the customer's transfer is valid, and once
fraud is back the transfer can be screened and released. `FAILED` would make a temporary dependency
outage into a permanent customer-visible rejection.

The guarantee that matters — `FRAUD_UNAVAILABLE` implies core post count 0 — is structural:
`FraudService.check` returns `UNAVAILABLE` on any error, `TransferService.convertAndPost` maps it to
a `save(...)` with status `PENDING_REVIEW` and never calls `coreGateway.post`. There is no code path
from a fraud failure to a posting.

An unrecognized fraud *decision* (something outside `ALLOW`/`DENY`/`REVIEW`) is treated the same way:
`REVIEW`, held for a human. The fraud service answering is not the same as it answering with something
we can act on.

## 7. Idempotency

The database is the synchronization boundary. `IdempotencyService.begin` inserts a placeholder row
(key, SHA-256 fingerprint, transfer id, no response) before any work. Migration
`V3__idempotency_constraints.sql` makes `idempotency_key` the primary key, so under concurrent
duplicates exactly one insert succeeds; the loser sees `DuplicateKeyException`, reads the winner's
row, and either replays its stored response (same fingerprint), gets `422 IDEMPOTENCY_KEY_REUSED`
(different fingerprint), or waits for it. The wait polls every 100 ms for 15 s, then answers
`409 REQUEST_IN_PROGRESS`.

One posting is therefore guaranteed: only the owner of the placeholder row runs the pipeline, and
the owner posts exactly once — a core timeout is not retried. All callers of the same key receive the
same logical result because they read the same stored response.

After a restart the row survives. A key whose response never arrived still answers 409, which is the
conservative truth: the request may genuinely still be in flight on a node we can no longer see.

With 3 replicas this scheme holds for correctness (the constraint is database-wide) but not for
liveness: the owner that died mid-pipeline leaves a row with no response, and the other replicas
answer 409 rather than taking over. To make that safe I would move ownership to a leased row —
`SELECT ... FOR UPDATE SKIP LOCKED` on a claim, or an advisory lock — so a replica can detect that
the owner is gone and complete the work. The README's "Going further" section points at exactly this.

## 8. Virtual threads

Why this design: the SDK is irreducibly blocking and synchronized, and the event loop must never
wait. A virtual thread per blocking call is the cheapest possible bridge — no pool to size, no
queue to bound — because the expensive resource is the core's ten sessions, and that is limited with
a semaphore, not a thread pool. `STACK.md` says it exactly: "Limits, not pools."

The hand-off is `Mono.fromCallable(() -> blockingCall()).subscribeOn(virtualThreadExecutor)` in
`DefaultCoreGateway.withPermit`. The result flows back into the Reactor pipeline as a `Mono`, so the
caller sees only reactive types. In the other direction, `ReconciliationService` is plain imperative
Java on virtual threads that consumes the reactive repository by blocking on it and publishes events
through `TransferEvents`' sink.

What the SDK's synchronized session I/O means on Java 21: a virtual thread that blocks inside
`synchronized` pins its carrier. With 2 CPUs, a handful of pinned carriers stalls every other virtual
thread. That is why the semaphore is sized to the core's own limit of 10 rather than to an assumed
thread budget: the SDK holds the session lock only for the duration of one socket call, and the
semaphore keeps the number of concurrent session objects at the level the core documents.

Measurement: the run in §3 and §10 held the core at a 5 s hang under load; requests continued to be
served and the health endpoint stayed responsive, and the trace shows the blocked call parked on
`virtual-45` while event-loop threads kept serving. `DefaultCoreGatewayTest` measures concurrency
directly (200 callers, high-water mark 10). I did not run a JFR `jdk.VirtualThreadPinned` recording;
that remains the honest caveat on these numbers, and it is the first thing I would measure before
trusting throughput claims.

## 9. Streams

**Batch.** `BatchController.batch` reads the request body buffer by buffer, splitting lines as they
arrive, and returns a `Flux` of result lines. Nothing collects the batch: `lines(exchange)` emits
per buffer, and the response is assembled by the framework as the flux emits. Per-account ordering is
`groupBy(sourceAccount)` → `concatMap(runLine)` inside a group, with `flatMap(groups,
accountConcurrency)` across groups (default 8). A line for an account starts only after the previous
line for that account has a status. A malformed line becomes a `Failure` item in a shared errors
group that maps straight to a result line, so it never stops the stream. Results stream in completion
order, which is what the contract asks for.

Slow clients and disconnects: demand is pulled by the response, so a client that stops reading stops
the pull and lines not yet started are never processed. Lines already in flight finish — they carry
their own idempotency key, so a replay by that key finds the same result rather than re-posting.

**Events.** `TransferEvents` is a hot `Sinks.many().multicast().directBestEffort()` sink. Producers
from event-loop threads (the transfer API) and from virtual threads (reconciliation) both call
`tryEmitNext`; the multicast sink serializes concurrent emits, so no lock is needed. Delivery runs on
`boundedElastic` (`stream():94`), so a subscriber's `onNext` never runs on the publishing thread —
that is what keeps a stalled client from stalling money movement.

Backpressure strategy: **slow subscribers lose events and nobody is blocked**. `directBestEffort`
drops to a subscriber that is not ready rather than buffering for it. A subscriber that keeps up
never loses or duplicates an event; a stalled one misses its oldest events. I chose this over
per-subscriber buffering because an unbounded buffer for a stalled client eventually exhausts memory
and then slows the transfers themselves — the wrong trade for a live feed. A heartbeat comment is
emitted every 15 s (`TransferEventsController`). On disconnect, `doFinally` decrements the
`transfer.events.subscribers` gauge and the sink drops everything the subscriber held, since nothing
is buffered for it.

## 10. Docker

Base image: multi-stage — `maven:3.9-eclipse-temurin-21` builds the reactor, `eclipse-temurin:21-jre`
runs it. Measured image size **347.8 MB**. The final stage contains only `/app/transfer-service.jar`;
no Maven, no compiler, no sources (`which mvn` inside the image: absent). Runs as `USER 10001:0`
(named `transfer`), verified in the running container.

Memory settings, against the 512 MB limit: `-XX:MaxRAMPercentage=60` (not `-Xmx1g`, which INC-107
made fatal), `-XX:MaxDirectMemorySize=96M` for Netty's off-heap buffers,
`-XX:MaxMetaspaceSize=128M`, `-XX:+ExitOnOutOfMemoryError`. Measured RSS at rest under load:
**233.6 MB of 512 MB** — headroom, not a cliff.

Graceful shutdown: `server.shutdown: graceful` with `spring.lifecycle.timeout-per-shutdown-phase: 20s`
and `STOPSIGNAL SIGTERM`. Measured `podman stop` of the running service: **5.1 s**, exit code 143 —
in-flight work finished well inside the grace period.

The full stack was run outside Compose with Compose's own wiring (podman has no compose provider in
this environment, so I reproduced it: one network, service-name aliases `postgres` / `mock-bank` /
`transfer-service`, the same environment variables, `--cpus 2 --memory 512m`). Health endpoint came
up, a cross-currency transfer returned `201` with a BigDecimal-precise IDR credit, SSE delivered an
event to a subscriber connected before the POST, the batch endpoint streamed per-line results with a
malformed line isolated, and reconciliation answered its stats shape. The one thing not verified is
`docker compose up --build` itself, because no compose provider is installed here; that is the gap to
close before submission.

## 11. Git and security

**SDK-107's conflict.** Both SDK fixes touch `CoreBankingClient.inquire`/`withSession`: SDK-101
removed the `reference.toUpperCase(Locale.ROOT)` uppercasing so `inquire` uses the reference as given,
and SDK-107 added `session.close()` to the `SocketTimeoutException` branch. Resolved so both survive:
`inquire` keeps the caller's case, and the timeout branch closes the session before throwing
`CoreTimeoutException`. The resolution is visible in `CoreBankingClient.java` today — the uppercase
call is gone, and both catch branches close the session.

Both came in with `git cherry-pick -x` (the messages carry `cherry picked from commit 4d839f7` and
`eb8e8d3`), along with the three useful test-kit commits (`MockBankContainer`, the StepVerifier
examples, the virtual-thread helpers). Nothing else from `platform/sdk-next` was merged.
`legacy-core-sdk/`, `mock-bank/` and `web-client/` are otherwise untouched.

**SEC-301.** A credential lived in `transfer-service/src/main/resources/application-local.yml`,
committed together with generated `target/` artifacts and an `.idea/` directory. Removed in
`cf0cb2b` and `f445d26`: the file, the generated artifacts and the IDE config were untracked, and a
`.gitignore` was added covering `application-local.yml`, `.env`, `*.key`, `*.pem`, `target/`,
`.idea/` and the rest. Today `git ls-files` shows none of them tracked.

What must happen outside the repository, because removing a file from the tip does not uncommit it:
the credential is in the object history of every clone and fork, so it must be treated as compromised
and **rotated or revoked** — that is an operational change on the system it authenticated against,
not a repository change. Anyone who cloned before the removal should re-clone or run a history
rewrite; since this fork is public, assume the secret was read.

## 12. Production

At 3 replicas: correctness holds where the database is the authority (idempotency's unique key,
account and balance checks in core) but liveness degrades as described in §7 — a request that died
mid-pipeline on one node leaves an unowned key that other replicas answer with 409 rather than
completing. Reconciliation also becomes redundant work without row locking: three instances each
inquire the same PENDING transfers, wasting core sessions. `FOR UPDATE SKIP LOCKED` would partition
the work. The SSE gauge and the semaphore are per-instance, so `transfer.events.subscribers` reports
one node's subscribers and the ten-session limit becomes 30 across three pods — the core's limit is
global, so a shared limiter (or core-side enforcement) is needed before that scale.

At 10× traffic: the semaphore keeps the core safe but queues callers, so P99 rises on the API while
the core stays at ten sessions. The batch endpoint's 1000-line cap and 8-way account concurrency are
the other bounds; a sustained flood of batches would saturate `boundedElastic` delivery for SSE.

With another week, in order: lease-based idempotency ownership and `FOR UPDATE SKIP LOCKED`
reconciliation (both unblock 3 replicas); a JFR pinning measurement to validate the virtual-thread
numbers; BlockHound in the test suite as a regression guard against event-loop blocking; a
circuit breaker around FX so a throttling provider fails fast instead of retrying; and a
docker-compose run in an environment that has a compose provider.

## 13. Learning

I started unfamiliar with the specific failure modes this assessment is about, and the repository
taught them. `docs/STACK.md`'s "limits, not pools" reframed concurrency for me: I had assumed thread
pools were the tool, and the SDK's ten-session constraint is the clearer way to think about it.
Reading `scripts/repro/INC-101.sh` before the code made the ambiguity of a timeout concrete — the
money moves while the caller sees failure — and that single fact explains most of the design here.
The mock bank's admin API was the fastest teacher: setting `hangRate` and `applyOnHangRate` and
reading `/__admin/stats` turned "is this safe?" into a countable number of postings. I also learned
that Java 21's synchronized pinning is a real hazard on few CPUs, which is why I am not presenting
the threading numbers without the JFR caveat.

## 14. AI

I used an AI assistant (Claude Code) for this work, and this section discloses that accurately rather
than claiming otherwise. It was used to: audit the repository against the assessment's requirements,
reading the source and reporting where the implementation did and did not meet each one; produce the
first drafts of this document; and generate test cases for the fraud fail-closed and `Retry-After`
cap fixes. The codebase was read and the fixes reviewed by a person before being applied; the
runs and measurements quoted here are real and were executed, not estimated. Where the assistant's
claim could not be verified — the JFR pinning measurement, the `docker compose up` run — §8, §10 and
§12 say so explicitly instead of asserting it.

## 15. Testing

What is tested at which level:

* **Unit (Mockito, StepVerifier).** The pipeline's decisions in `TransferServiceTest`; the decision
  matrix in `ReconciliationServiceTest`; idempotency's race handling in `IdempotencyServiceTest`;
  retry classification in `HttpRetriesTest`; fraud outcomes in `FraudServiceTest`; rounding in
  `MoneyTest` and `FxServiceTest`; request validation in `TransferRequestsTest`.
* **Virtual time.** `HttpRetriesTest` and `FxServiceTest` use `StepVerifier.withVirtualTime`, so a
  5 s `Retry-After` or an exponential backoff is observed as a virtual advance, never a real wait.
* **Concurrency.** `DefaultCoreGatewayTest` uses latches and barriers — 200 callers against the
  semaphore, a slow call behind a latch — and asserts the thread is virtual. No test uses
  `Thread.sleep` for synchronization (`grep` in the test tree returns nothing).
* **Web layer.** `ProblemWebExceptionHandlerTest` and the controller tests assert status, content
  type and error code through `WebTestClient`.
* **Integration.** `MockBankContainerTest` builds the mock bank image and runs it in a real
  container, exercising its admin API. `TransferEndToEndTest` goes further: the whole service
  (WebFlux, R2DBC, Flyway, WebClient, the core gateway) runs against the mock bank and Postgres
  both in real containers, with nothing mocked. It proves the two incidents a mocked test cannot:
  a core that hangs past the SDK timeout and applies the posting anyway is posted to exactly once
  and returns `202 PENDING / CORE_TIMEOUT`, and a fraud 503 results in zero core postings. It also
  asserts the cross-currency credit is rounded to the cent. Both are guarded by
  `@Testcontainers(disabledWithoutDocker = true)`, so they run where Docker exists and are skipped,
  not failed, where it does not.

`./mvnw verify` runs all of it plus the JaCoCo check: **201 tests, all passing** (5 container tests
skipped when Docker is unavailable), **91.08% line coverage** against the required 80%. With Docker
available the two container suites add 5 passing tests.

What coverage misses: the SSE heartbeat and a genuinely slow subscriber are untested against a real
connection, and the concurrency tests prove the semaphore but not the full batch ordering guarantee
(same-account serialization) under contention.
