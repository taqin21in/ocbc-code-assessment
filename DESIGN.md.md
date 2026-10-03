# Design

This document covers the 15 sections from `docs/DESIGN\_TEMPLATE.md`. I kept the explanations focused on the problems I found in the repository, the changes I made, and the tests or runs I used to verify them.

## 1\. Problems found

I found a few problems in the original implementation beyond the incident list. The table below summarizes the main ones, including where they came from, what I changed, and how I checked the result.

|Problem|File / cause|Fix|Test|
|-|-|-|-|
|The whole transfer pipeline was retried, including the Core post|`TransferService` used `.retry(3)` around the whole flow. If the Core post had already been applied and the client timed out, the whole flow could run again. That could debit the customer twice (INC-101) and still leave the transfer marked `FAILED` even though the money had moved (INC-109).|I made Core timeout terminal for the request path: the transfer is stored as `PENDING` with `CORE\_TIMEOUT` and the API returns HTTP 202. Only `CoreBusyException` and `CoreUnavailableException`, where the SDK says nothing was sent, are retried, and those retries live inside `DefaultCoreGateway`.|`TransferServiceTest.coreTimeoutBecomesPendingAndIsNeverReposted` checks one post, 202, `PENDING`, and `CORE\_TIMEOUT`. I also reproduced the case end to end in §3.|
|Core calls ran on the event loop|The blocking SDK was called directly from the reactive pipeline (INC-102).|`DefaultCoreGateway.withPermit` wraps the SDK call in `Mono.fromCallable(...).subscribeOn(virtualThreadExecutor)`. `VirtualThreads` owns the single virtual-thread executor.|`DefaultCoreGatewayTest.aSlowCoreCallDoesNotBlockTheSubscriberThread`|
|There was no global Core session limit|API callers and reconciliation could open their own sessions. Under load, reconciliation could trigger Core "session busy" errors (INC-108).|I use one fair `Semaphore(10)` in `DefaultCoreGateway`. The same gateway is used by the API, batch, and reconciliation, so they share the same ten permits.|`DefaultCoreGatewayTest.twoHundredCallersNeverExceedTenInFlight`|
|FX retried everything three times|`FxClient` used `.retry(3)` for every error and ignored `Retry-After` (INC-103).|`HttpRetries.classified` retries 429 using `Retry-After` with a cap, retries 5xx with bounded exponential backoff, and does not retry other 4xx responses.|`HttpRetriesTest` uses virtual time to check 429, 500, 404, and the retry limit.|
|Fraud failures became `ALLOW`|The original code caught fraud errors, logged a warning, and returned `ALLOW` (INC-104).|`FraudService` maps failures to `FraudOutcome.UNAVAILABLE`. The transfer is kept as `PENDING\_REVIEW` with `FRAUD\_UNAVAILABLE`, and no Core post is made.|`TransferServiceTest.fraudUnavailableHoldsTheTransferAndPostsNothing`|
|Unknown fraud decisions could release money|`FraudService.toOutcome` treated an unknown decision as `ALLOW`. That was fail-open for a value the fraud contract does not define.|I treat an unknown decision as `REVIEW`, so a human decision is required and no money moves.|`FraudServiceTest.anUnknownDecisionIsHeldForReview`; `TransferServiceTest.fraudReviewHoldsTheTransferAndPostsNothing`|
|Idempotency used SELECT-then-INSERT|Two concurrent requests could both see a missing key and both insert it (INC-105). The fingerprint also used `hashCode()`.|`IdempotencyService.begin` inserts the placeholder first. The V3 primary key makes one insert win. The fingerprint is SHA-256 over canonical JSON through `RequestFingerprint`.|`IdempotencyServiceTest`; `V3\_\_idempotency\_constraints.sql`|
|FX credit amounts could be off by 0.01|`FxService` used `double` and `Math.round(...)` (INC-106).|The calculation stays in `BigDecimal` and uses `setScale(2, RoundingMode.HALF\_EVEN)`.|`MoneyTest` rounding cases; `FxServiceTest`|
|Reconciliation re-posted timed-out transfers|`ReconciliationJob` used a fixed 50-thread pool and posted the transfer again.|I replaced it with `ReconciliationService`. It uses inquiry only, runs on virtual threads, and allows only one reconciliation pass at a time.|`ReconciliationServiceTest` covers the decision matrix.|
|Errors were not returned in a consistent format|The web client could get an "Unrecognised error response", sometimes with a stack trace (INC-111).|`ProblemWebExceptionHandler` returns RFC 9457 `application/problem+json` with `code` and `correlationId`, without stack traces or class names.|`ProblemWebExceptionHandlerTest`|
|There was no correlation ID|It was difficult to follow one request through the logs (INC-111).|`CorrelationIdFilter` keeps an incoming `X-Correlation-Id` or generates one, returns it, and puts it in Reactor context. `ContextPropagation` carries it into MDC.|`CorrelationIdFilterTest`; the trace in §3|
|`Retry-After` could delay a transfer for an unlimited amount of time|`HttpRetries` used the header without any upper limit.|I added `http.retry-after-max` with a default of 30 seconds and wait for the smaller of the header value and the cap.|`HttpRetriesTest.aHostileRetryAfterIsCapped`|
|The container could be OOMKilled|The service used `-Xmx1g` while the container limit was 512 MB (INC-107).|I changed the image to a multi-stage build, use container-aware memory settings, and run as a non-root JRE image.|See §10.|
|Services used `localhost` in Compose|Inside a container, `localhost` points back to the same container (INC-110).|I switched the configuration to service-name DNS and environment variables in `docker-compose.yml`.|Verified with the stack run described in §10.|

## 2\. Threads

I ended up with three different thread types in the service.

**Netty event loop** (`reactor-http-epoll-\*`, `reactor-tcp-epoll-\*`). Controllers, WebClient calls to accounts/FX/fraud, R2DBC, and the SSE output run here. I keep blocking work off these threads. There is no `.block()`, blocking I/O, or `Thread.sleep` on this path. The only hand-off from the event loop is to `boundedElastic`, used once in `TransferEvents.stream():94`, so a subscriber's `onNext` does not run on the publishing thread.

**Virtual threads** (`virtual-\*`). I use one executor from `VirtualThreads.virtualThreadExecutor()`, based on `newVirtualThreadPerTaskExecutor()`. The main users are `DefaultCoreGateway.withPermit`, for every blocking Core SDK call, and `ReconciliationService`, which runs the reconciliation pass and inquiry work on virtual threads.

**`boundedElastic`** is only used for SSE subscriber delivery.

The blocking parts are deliberately kept in two places:

* `CoreBankingClient.post` / `inquire` use synchronized session I/O over `HttpURLConnection`, and they run on virtual threads inside `DefaultCoreGateway`.
* `ReconciliationService.reconcile:115,179,218` uses `.block()` on the reactive repository and gateway, but only from its virtual-thread worker. This matches the assessment's requirement that the reconciliation worker may consume the reactive repository from virtual threads.

A real run in §3 shows the Core post on `virtual-45` while the request itself starts on an event-loop thread.

## 3\. Trace

For the trace, I used one transfer that became `PENDING`. The caller sent correlation ID `trace-pending-001`. The mock bank was configured to hang Core for 5 seconds but apply the posting anyway, which is the INC-101 case.

```text
19:52:28.800 INFO \[trace-pending-001] \[or-http-epoll-4] TransferController   : Transfer request with Idempotency-Key pending-repro-001
19:52:29.516 INFO \[trace-pending-001] \[tor-tcp-epoll-2]  TransferService     : Creating transfer 325ebaaa-…: 250.00 USD from 2000000001 to 1000000002
19:52:31.841 INFO \[trace-pending-001] \[or-http-epoll-2]  TransferService     : Posting transfer 325ebaaa-… through core banking
19:52:34.704 WARN \[trace-pending-001] \[     virtual-45] TransferService     : Core banking did not answer for transfer 325ebaaa-…; leaving it PENDING for reconciliation
19:52:34.753 INFO \[trace-pending-001] \[     virtual-45] TransferService     : Transfer 325ebaaa-… is PENDING CORE\_TIMEOUT
19:52:50.329 INFO \[-               ] \[     virtual-52] ReconciliationService: Transfer 325ebaaa-… is POSTED in core banking: COMPLETED
```

The API returned `202` with `status=PENDING` and `reasonCode=CORE\_TIMEOUT`. The mock bank `/\_\_admin/stats` showed exactly one posting for the reference. Fifteen seconds later, reconciliation inquired, found the posting, and changed the transfer to `COMPLETED` with `coreTxnId=CT00000001`. There was no second post.

The thread names are also useful here: the request entered through event-loop threads, the Core call ran on `virtual-45`, and reconciliation ran on another virtual thread. The scheduled reconciliation pass has no request correlation ID, so it uses the pass-scoped `reconciliation-N` MDC value instead.

## 4\. Core SDK

All SDK calls go through the single `DefaultCoreGateway` bean. It owns one fair `Semaphore(properties.coreMaxSessions())`, which defaults to 10 in `application.yml:40`.

Because the transfer API, batch, and reconciliation all use the same gateway, they also use the same ten permits. I checked the codebase for additional semaphores and found none outside the gateway.

With 200 concurrent requests, all 200 virtual threads can move up to the Core call, but only ten can acquire the permit and open a session. The remaining requests wait for a permit. When a call finishes, `finally` releases the permit and the next caller can proceed. The Core side therefore never sees more than ten open sessions.

`DefaultCoreGatewayTest.twoHundredCallersNeverExceedTenInFlight` verifies the high-water mark is exactly 10. `DefaultCoreGatewayTest.aPermitIsReleasedAfterATimeout` also verifies that a timeout does not leak a permit.

I used `acquireUninterruptibly()` because I want the permit accounting to stay consistent if a carrier thread is interrupted. The corresponding `release` is always in `finally`.

## 5\. Timeouts and retries

These are the current defaults from `application.yml`.

|Dependency|Timeout|Retries|Backoff|Retried statuses|Why|
|-|-|-|-|-|-|
|Accounts|5 s per attempt|4 attempts total|429: `Retry-After` (capped 30 s); 5xx: 100 ms × 2^n|429, 5xx, read timeout|Read-only, so repeating the request is safe|
|FX|5 s per attempt|4 attempts total|Same as Accounts|429, 5xx, read timeout|`Retry-After` is honoured; other 4xx responses are not retried|
|Fraud|1.5 s|none|—|none|The fraud provider caps client patience at 1.5 s. Retrying only delays the fail-closed decision.|
|Core (post)|2 s read|3 attempts|100 ms × 2^n|`CoreBusyException`, `CoreUnavailableException` only|These mean the request was not sent. A timeout does not give me that guarantee.|
|Core (inquire)|2 s read|3 attempts|Same as post|Same as post|Inquiry is read-only and safe to repeat.|

For one transfer, the worst-case serial time is about 51.6 seconds plus database round trips. Accounts and FX may run in parallel where the pipeline allows it. Large `Retry-After` values are capped at 30 seconds per retry, and callers can also wait for a Core semaphore slot when all ten sessions are busy.

The most important rule here is that **Core POST timeout is not retried**. That is the rule that prevents INC-101.

## 6\. Fraud unavailable

I chose `PENDING\_REVIEW` with `FRAUD\_UNAVAILABLE` and HTTP 202.

The reason is that a fraud outage is not the same as a fraud decision. I do not want a temporary dependency problem to become a permanent customer rejection, but I also do not want to release the money while fraud screening is unavailable.

The important safety property is:

```text
FRAUD\_UNAVAILABLE -> Core POST count = 0
```

This is enforced by the code path itself. `FraudService.check` turns any fraud error into `UNAVAILABLE`. `TransferService.convertAndPost` saves the transfer as `PENDING\_REVIEW` and does not call `coreGateway.post` for that outcome.

For the same reason, an unknown fraud decision is handled as `REVIEW` instead of `ALLOW`. If the fraud provider sends a value outside `ALLOW` / `DENY` / `REVIEW`, I would rather hold the transfer than guess what the value means.

## 7\. Idempotency

I use the database as the synchronization boundary for idempotency.

`IdempotencyService.begin` creates a placeholder row with the key, SHA-256 fingerprint, transfer ID, and no response before the transfer work starts. `V3\_\_idempotency\_constraints.sql` makes `idempotency\_key` the primary key. That means concurrent requests cannot both create the same key.

The losing request handles `DuplicateKeyException`, reads the winner's row, and then does one of three things:

* same fingerprint and completed response -> replay the stored response;
* different fingerprint -> return `422 IDEMPOTENCY\_KEY\_REUSED`;
* same key but no completed response yet -> wait for the owner for up to 15 seconds, then return `409 REQUEST\_IN\_PROGRESS`.

The request fingerprint is SHA-256 over canonical JSON. I no longer use `hashCode()` for it.

This means only the owner of the idempotency row runs the transfer pipeline, and a Core timeout does not cause that owner to post again. All callers using the same key see the same logical result.

After a restart, the database row is still there. If the owner disappeared before storing a response, another request returns 409 rather than assuming it is safe to run the transfer again. That is intentionally conservative.

For three replicas, this design keeps the correctness property because the unique key is enforced by the database, but it does not fully solve liveness. An owner that dies mid-pipeline leaves an uncompleted row that other replicas cannot safely take over. A future improvement would be a leased ownership model using `SELECT ... FOR UPDATE SKIP LOCKED` or an advisory lock.

## 8\. Virtual threads

The reason for using virtual threads here is fairly simple: the Core SDK is blocking, but the WebFlux event loop must not block.

I use a virtual thread for the blocking Core call instead of introducing another fixed-size pool. The expensive resource is the Core session, so I limit that with a semaphore rather than using a large thread pool. This follows the "limits, not pools" idea from `docs/STACK.md`.

The hand-off is:

```java
Mono.fromCallable(() -> blockingCall())
    .subscribeOn(virtualThreadExecutor)
```

inside `DefaultCoreGateway.withPermit`.

The normal API still returns Reactor types. For reconciliation, the worker is intentionally imperative Java running on virtual threads and can therefore block on the reactive repository and gateway.

There is one Java 21 detail I am keeping in mind: the SDK uses synchronized session I/O. A virtual thread can pin its carrier while it is blocked inside synchronized code. With only 2 CPUs, enough pinned carriers could hurt throughput. The semaphore keeps the number of concurrent Core sessions to the documented limit, but I did not run a JFR `jdk.VirtualThreadPinned` recording. That is still something I would measure before making throughput claims.

The real run in §3 showed the blocked Core call on `virtual-45` while the event-loop threads stayed responsive. `DefaultCoreGatewayTest` also checks the concurrency limit directly with 200 concurrent callers.

## 9\. Streams

### Batch

The batch endpoint reads the request body incrementally and returns a `Flux` of result lines. I do not collect the whole request first.

The processing model is:

```text
groupBy(sourceAccount)
    -> concatMap(runLine) within each account
    -> flatMap(groups, accountConcurrency) across accounts
```

The default account concurrency is 8.

This gives me two things at the same time:

* lines for the same source account stay in order;
* different source accounts can run in parallel.

A malformed line becomes a failure result for that line instead of terminating the whole batch. Results are streamed in completion order, which matches the contract.

For disconnects, backpressure comes from the response demand. If the client stops reading, lines that have not started yet do not need to be processed. Lines already in flight can finish, and their idempotency keys make a later replay safe.

### Events / SSE

`TransferEvents` uses a hot `Sinks.many().multicast().directBestEffort()` sink. Events can be published by both the transfer API and reconciliation. Subscriber delivery is moved to `boundedElastic` so a slow subscriber does not run `onNext` on the publishing thread.

The backpressure decision is intentional: **a slow subscriber may miss events, but it must not slow down transfers**. I chose `directBestEffort` instead of keeping an unbounded buffer per subscriber, because an unbounded buffer for a stuck client can eventually become a memory problem for the service itself.

The stream also sends a heartbeat comment every 15 seconds. When a client disconnects, `doFinally` decrements the `transfer.events.subscribers` gauge. Nothing is replayed to a new subscriber.

## 10\. Docker

The final image is a multi-stage build. Maven and the JDK are only used in the build stage, and the runtime stage uses `eclipse-temurin:21-jre`.

The runtime image contains only the application JAR. There is no Maven, compiler, or Java source in the final stage. The container runs as `USER 10001:0` (`transfer`, UID 10001). The measured image size was 365 MB.

For the 512 MB memory limit, I replaced the original `-Xmx1g` configuration with:

* `-XX:MaxRAMPercentage=60`
* `-XX:MaxDirectMemorySize=96M`
* `-XX:MaxMetaspaceSize=128M`
* `-XX:+ExitOnOutOfMemoryError`

During the stack run, the cgroup limit was 512 MiB and the process used about 241.6 MB; the measured `VmRSS` was about 254 MB.

Graceful shutdown is enabled with:

```yaml
server.shutdown: graceful
spring.lifecycle.timeout-per-shutdown-phase: 20s
```

and the container receives `SIGTERM`. A `podman stop` run completed shutdown in about 3.4 seconds.

I could not run `docker compose up --build` directly because a Compose provider was not available in the environment I used. I reproduced the same network and environment setup with Podman: `postgres`, `mock-bank`, and `transfer-service` were connected through service-name aliases with the same `--cpus 2 --memory 512m` limits.

That run covered the important scenarios: health came up, a cross-currency transfer returned `201` with the expected BigDecimal result (`150.00 × 16250.5037 -> 2437575.56`), a Core hang returned `202 PENDING / CORE\_TIMEOUT` and resulted in exactly one Core posting before reconciliation completed it, a fraud 503 returned `PENDING\_REVIEW / FRAUD\_UNAVAILABLE` with no extra Core posting, SSE delivered the transfer event and 15-second heartbeat, batch results were streamed with malformed lines isolated, reconciliation returned its summary and `409 RECONCILIATION\_RUNNING` when started twice, and validation errors returned `application/problem+json` with a correlation ID and no stack trace.

The one remaining verification is to run `docker compose up --build` on a machine that has a Compose provider installed.

## 11\. Git and security

### SDK cherry-pick conflict

SDK-101 and SDK-107 both touched `CoreBankingClient.inquire` / `withSession`.

SDK-101 removed `reference.toUpperCase(Locale.ROOT)`, so `inquire` keeps the reference exactly as supplied. SDK-107 added `session.close()` in the `SocketTimeoutException` branch before throwing `CoreTimeoutException`.

I resolved the conflict so both changes remain. The uppercase conversion is gone, and the timeout branch closes the session before throwing.

Both commits were brought in with `git cherry-pick -x` (`4d839f7` and `eb8e8d3`). I also cherry-picked the three test-kit commits used for `MockBankContainer`, StepVerifier examples, and the virtual-thread helpers. I did not merge the rest of `platform/sdk-next`. `legacy-core-sdk/`, `mock-bank/`, and `web-client/` were otherwise left unchanged.

### SEC-301

The repository originally contained a credential in `transfer-service/src/main/resources/application-local.yml`, along with generated `target/` files and `.idea/` content. I removed them in commits `cf0cb2b` and `f445d26` and added a `.gitignore` covering `application-local.yml`, `.env`, `\*.key`, `\*.pem`, `target/`, `.idea/`, and related generated files.

Today `git ls-files` no longer shows those paths as tracked.

The secret still exists in Git history, so removing the file from the current tree is not enough. I am treating that credential as compromised and it should be rotated or revoked outside the repository. Anyone who cloned the repository before the cleanup should re-clone or rewrite history as appropriate.

## 12\. Production

At three replicas, the database still gives me the main correctness guarantees because idempotency uses a database unique key and the account/balance checks are enforced by Core. The main weakness is liveness.

If one replica dies after claiming an idempotency key, another replica currently returns 409 instead of taking over that request. Reconciliation also becomes duplicate work if each replica can inquire the same PENDING transfer. `FOR UPDATE SKIP LOCKED` would let the workers divide the reconciliation work safely.

The SSE subscriber gauge and the semaphore are per instance. That means three replicas could report three separate subscriber counts and could collectively allow 30 Core sessions even though the Core limit is 10 globally. Before scaling this design, I would add a shared limiter or rely on a Core-side global limit.

At 10× traffic, the Core semaphore still protects the dependency, but requests queue and API P99 will rise. Batch size is capped at 1000 lines and account-level concurrency is capped at 8, which gives two more backpressure points. SSE delivery could become another resource pressure if there are many subscribers.

With another week, my next priorities would be:

1. lease-based idempotency ownership;
2. `FOR UPDATE SKIP LOCKED` for reconciliation;
3. a JFR measurement for virtual-thread pinning;
4. BlockHound in tests to catch event-loop blocking regressions;
5. a circuit breaker around FX;
6. a real `docker compose` run in an environment with Compose available.

## 13\. Learning

The main thing I learned from this assessment is that a timeout does not automatically mean a transaction failed.

The `INC-101.sh` reproduction made that very clear: the client can see a timeout while the money has already moved. That single case affects retry design, transfer status, reconciliation, and idempotency.

I also changed the way I think about concurrency after reading `docs/STACK.md`. Instead of starting with a thread-pool size, I started with the real resource limit: the Core system allows ten sessions. That led to the semaphore design.

The mock bank's admin endpoints were also useful because I could turn the failure scenarios into something measurable. For example, I could configure a Core hang and then check `/\_\_admin/stats` to see whether the reference was posted once or more than once.

Finally, I learned to be more careful with Java 21 virtual threads when the underlying library uses synchronized blocking I/O. They solve the event-loop problem, but they do not remove the need to understand carrier-thread pinning.

## 14\. AI

I used an AI assistant (Chat GPT) during this work. I am documenting that here instead of presenting the work as entirely manual.

I used it mainly to:

* audit the repository against the assessment requirements;
* review the source code and point out places that did not meet those requirements;
* help draft the first version of this document; and
* generate some of the test cases for the fraud fail-closed behavior and the `Retry-After` cap.

I reviewed the changes before applying them and ran the tests and measurements myself. The numbers in this document are from actual runs. Where I could not verify something, I left that limitation in the document instead of presenting it as a measured result. In particular, I did not run the JFR `jdk.VirtualThreadPinned` recording, and I did not run `docker compose up --build` in an environment with a Compose provider.

## 15\. Testing

I used several test levels so the important failure modes are not covered only by unit tests.

### Unit tests

The main unit coverage is split across:

* `TransferServiceTest` for transfer decisions;
* `ReconciliationServiceTest` for the reconciliation decision matrix;
* `IdempotencyServiceTest` for duplicate/race handling;
* `HttpRetriesTest` for retry classification;
* `FraudServiceTest` for fraud outcomes;
* `MoneyTest` and `FxServiceTest` for rounding;
* `TransferRequestsTest` for input validation.

### Virtual time

`HttpRetriesTest` and `FxServiceTest` use `StepVerifier.withVirtualTime`, so a 5-second `Retry-After` or exponential backoff can be tested without making the test wait in real time.

### Concurrency

`DefaultCoreGatewayTest` uses latches and barriers to test 200 callers against the semaphore and to test a slow Core call. It also checks that the blocking work is on a virtual thread. There is no `Thread.sleep` used for synchronization in the test tree.

`IdempotencyConcurrencyTest` sends 100 concurrent requests with the same body and `Idempotency-Key` against real Postgres. The test then checks the mock bank posting count and confirms that exactly one Core posting was made. The requests are released together using a reactive barrier through `Flux.merge`, not by waiting on a clock.

### Web layer

`ProblemWebExceptionHandlerTest` and the controller tests use `WebTestClient` to verify HTTP status, content type, and error codes.

### Integration

`MockBankContainerTest` starts the mock bank in a real container and exercises its admin API.

`TransferEndToEndTest` goes further: WebFlux, R2DBC, Flyway, WebClient, and the Core gateway all run against real Postgres and mock-bank containers. It verifies the cases that are difficult to prove with mocks alone:

* a Core call that hangs past the SDK timeout but still applies the posting is posted exactly once and returns `202 PENDING / CORE\_TIMEOUT`;
* a fraud 503 results in zero Core postings;
* cross-currency credit is rounded to the cent.

The container tests use `@Testcontainers(disabledWithoutDocker = true)`, so they run when Docker is available and are skipped otherwise.

`./mvnw verify` runs the full suite and the JaCoCo gate. The current run reported **202 tests, all passing**, with 5 container tests skipped when Docker was unavailable, and **92.2% line coverage** against the required 80% threshold.

The remaining coverage gap is practical rather than a core business-path gap: the SSE heartbeat and a genuinely slow subscriber are not yet tested through a real connection, and the concurrency suite proves the global semaphore limit but does not yet prove the complete same-account batch ordering guarantee under contention.

