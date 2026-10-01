# Transfer service API

This is the agreed contract. The implementation in this repository drifted from it: make the implementation match the contract, not the other way round.

## Conventions

- JSON in and out, UTF-8.
- Money is a decimal string with two places, for example `"150.00"`, together with its currency. Currencies: `IDR`, `USD`, `SGD`.
- Timestamps are ISO-8601 in UTC, for example `"2026-03-02T02:15:30.123456Z"`.
- Every response carries `X-Correlation-Id` (see below).

## Endpoints

| Endpoint | Purpose | In this repository |
|---|---|---|
| `POST /api/v1/transfers` | Create a transfer | Exists, does not follow this contract |
| `GET /api/v1/transfers/{transferId}` | Read a transfer | Exists |
| `POST /api/v1/transfers/batch` | Many transfers in one request (FEAT-201) | Build it |
| `POST /internal/reconciliation/run` | Run one reconciliation pass (FEAT-202) | Build it |
| `GET /api/v1/transfers/events` | Live status changes (FEAT-203) | Build it |
| `GET /actuator/health` | Health check | Exists |

## POST /api/v1/transfers

### Headers

| Header | Rules |
|---|---|
| `Idempotency-Key` | Required. 8 to 64 characters: letters, digits, `_` or `-`. Kept for 24 hours. |
| `X-Correlation-Id` | Optional. Generated if absent. Returned in the response and present in every log line for the request. |

### Body

```json
{
  "sourceAccount": "2000000001",
  "destinationAccount": "1000000002",
  "amount": "150.00",
  "currency": "USD",
  "description": "Invoice 42"
}
```

| Field | Rules |
|---|---|
| `sourceAccount`, `destinationAccount` | Required. 10 digits. Must differ. |
| `amount` | Required. A decimal string greater than 0, with at most 2 decimal places, at most 1,000,000,000.00. |
| `currency` | Required. `IDR`, `USD` or `SGD`. Must equal the source account's currency. |
| `description` | Optional. At most 140 characters. |

### Processing

1. **Validate.** Any failure: 400, nothing is stored.
2. **Idempotency.**
   - Same key and same body as an earlier request: replay the stored response, with the same HTTP status and the same body.
   - Same key, different body: 422 `IDEMPOTENCY_KEY_REUSED`.
   - Same key while the first request is still running: wait for its result, or answer 409 `REQUEST_IN_PROGRESS`.
3. **Fetch both accounts in parallel.**
4. **Convert currency** if the source and destination currencies differ: credit = amount × rate (source to destination), rounded `HALF_EVEN` to 2 places. Never use an expired rate.
5. **Fraud check** when the amount is at least the threshold for the source currency:

   | Currency | Threshold | Environment override |
   |---|---|---|
   | IDR | 75,000,000.00 | `FRAUD_THRESHOLD_IDR` |
   | USD | 3,000.00 | `FRAUD_THRESHOLD_USD` |
   | SGD | 6,500.00 | `FRAUD_THRESHOLD_SGD` |

   If the fraud service is unavailable, you choose the behaviour and justify it in `DESIGN.md`, but **no money may move**.
6. **Post through the core banking SDK.** The posting reference is the `transferId`. After a `CoreTimeoutException`, **never post again**: the transfer becomes PENDING and reconciliation resolves it.

### Outcomes

| Outcome | HTTP | `status` | `reasonCode` |
|---|---|---|---|
| Posted | 201 | `COMPLETED` | `null` |
| Fraud asks for review | 202 | `PENDING_REVIEW` | `FRAUD_REVIEW` |
| Fraud unavailable (your choice; no posting) | 202 or 503 | `PENDING_REVIEW` or `FAILED` | `FRAUD_UNAVAILABLE` |
| Core outcome unknown | 202 | `PENDING` | `CORE_TIMEOUT` |
| Account not found or not active | 422 | `REJECTED` | `ACCOUNT_NOT_FOUND` or `ACCOUNT_NOT_ACTIVE` |
| Currency mismatch or insufficient funds | 422 | `REJECTED` | `CURRENCY_MISMATCH` or `INSUFFICIENT_FUNDS` |
| Fraud denies, or the core rejects | 422 | `REJECTED` | `FRAUD_DENIED` or `CORE_REJECTED` |
| A dependency is down after retries | 503 | `FAILED` | `DEPENDENCY_UNAVAILABLE` |

### Transfer resource

Returned for every outcome above.

```json
{
  "transferId": "8c1f0a52-6b1d-4d0e-9f4e-3c2b7a9d1e55",
  "status": "COMPLETED",
  "reasonCode": null,
  "sourceAccount": "2000000001",
  "destinationAccount": "1000000002",
  "debit":  { "amount": "150.00",     "currency": "USD" },
  "credit": { "amount": "2437575.56", "currency": "IDR" },
  "fxRate": "16250.5037",
  "coreTxnId": "CT00000042",
  "createdAt": "2026-03-02T02:15:30.123456Z",
  "updatedAt": "2026-03-02T02:15:30.456789Z"
}
```

`fxRate` is `null` when no conversion was needed. `credit` and `fxRate` may be `null` when the transfer ended before conversion.

### Errors

Every other error, unexpected ones included, is `application/problem+json` ([RFC 9457](https://www.rfc-editor.org/rfc/rfc9457)) with `code`, `detail` and `correlationId`:

```json
{
  "type": "about:blank",
  "title": "Invalid request",
  "status": 400,
  "detail": "amount must be greater than 0.",
  "instance": "/api/v1/transfers",
  "code": "VALIDATION_ERROR",
  "correlationId": "5d2e8f14-0b7a-4c43-8a3e-2f6d9c1b7e20"
}
```

| HTTP | `code` | When |
|---|---|---|
| 400 | `VALIDATION_ERROR` | Invalid request, malformed JSON included |
| 404 | `TRANSFER_NOT_FOUND` | Unknown `transferId` |
| 409 | `REQUEST_IN_PROGRESS` | Same Idempotency-Key, first request still running |
| 422 | `IDEMPOTENCY_KEY_REUSED` | Same Idempotency-Key, different body |
| 500 | `INTERNAL_ERROR` | Anything unexpected |

Error responses never contain stack traces, exception class names or internal hostnames. The web client displays exactly these fields.

### Correlation ID

Every response carries `X-Correlation-Id`: the caller's value when it sent one, otherwise a new one. The same value is the `correlationId` of a problem response and appears on every log line written for the request.

## GET /api/v1/transfers/{transferId}

200 with the transfer resource, or 404 `TRANSFER_NOT_FOUND`.

## POST /api/v1/transfers/batch (FEAT-201)

### Request

`Content-Type: application/x-ndjson`, up to 1,000 lines. Each line is a JSON object:

```json
{"lineId": "L1", "idempotencyKey": "batch-2026-03-02-0001", "sourceAccount": "2000000001", "destinationAccount": "1000000002", "amount": "150.00", "currency": "USD", "description": "Invoice 42"}
```

### Response

200, `Content-Type: application/x-ndjson`, one line per input line, **in completion order**:

```json
{"lineId":"L1","httpStatus":201,"transfer":{ … transfer resource … }}
{"lineId":"L7","httpStatus":400,"error":{"code":"VALIDATION_ERROR","message":"amount must be greater than 0."}}
{"lineId":null,"lineNumber":9,"httpStatus":400,"error":{"code":"VALIDATION_ERROR","message":"Line 9 is not valid JSON."}}
```

A line that cannot be parsed gets `"lineId": null` and its `lineNumber`.

### Rules

- `Flux` end to end: read the request body as a `Flux` and return a `Flux`.
- Lines with the same `sourceAccount` run one at a time, in input order: a line starts only after the previous line for that account has a status. Lines for different accounts run concurrently.
- Results stream as they complete. Never collect the whole batch first.
- Concurrency is bounded and respects the limits of the systems you call.
- Each line goes through the same pipeline and idempotency rules as a single transfer.
- One bad line never stops the stream.
- If the client disconnects, lines not yet started are not processed.

## Reconciliation (FEAT-202)

### POST /internal/reconciliation/run

Runs one pass and answers when it is done:

```json
{"checked": 12, "completed": 7, "rejected": 1, "failed": 2, "stillPending": 2, "durationMs": 1840}
```

If a pass is already running: 409 `RECONCILIATION_RUNNING`.

A pass also runs automatically every `RECONCILIATION_INTERVAL_SECONDS` (default 30).

### Rules

- The worker reads PENDING transfers from the reactive `TransferRepository` (`Flux`) and blocks on it from its virtual threads. Blocking is fine there; it never is on the event loop.
- For each PENDING transfer, call `inquire(transferId)`:
  - found and POSTED: COMPLETED, and store `coreTxnId`;
  - found and REJECTED: REJECTED, `CORE_REJECTED`;
  - not found, and PENDING for at least `RECONCILIATION_GRACE_SECONDS` (default 10): FAILED, `NOT_POSTED`;
  - not found and younger, or the inquiry failed: stays PENDING.
- **Never post again.**
- Every status change publishes an event (FEAT-203).
- Plain imperative Java on **virtual threads**. The only Reactor types the worker touches are the repository's `Flux`, consumed by blocking, and the event sink.
- Core concurrency, API path and reconciliation together, never exceeds 10 sessions.
- One failing transfer never affects the others.
- Shuts down cleanly.

## GET /api/v1/transfers/events (FEAT-203)

`text/event-stream`, with an optional filter `?sourceAccount=2000000001`. `Flux` end to end.

One event when a transfer first gets a status, and one for every later change, from the API path and from reconciliation:

```
event: transfer-status
id: 8c1f0a52-6b1d-4d0e-9f4e-3c2b7a9d1e55:2
data: {"transferId":"8c1f0a52-6b1d-4d0e-9f4e-3c2b7a9d1e55","status":"COMPLETED","reasonCode":null,"updatedAt":"2026-03-02T02:15:30.456789Z"}
```

The `id` is `<transferId>:<sequence>`; the sequence numbers the changes of one transfer.

### Rules

- Hot stream: subscribers get events from the moment they connect. No replay.
- Events are published from event-loop threads and from virtual threads at the same time. A subscriber that keeps up never loses or duplicates an event.
- A slow or stalled subscriber must never slow down transfers, reconciliation or other subscribers. It may lose its oldest events or be disconnected: document which.
- A heartbeat comment every 15 seconds.
- A disconnect releases everything the subscriber held.
- The number of subscribers is the Actuator gauge `transfer.events.subscribers`.

## Configuration

All settings come from environment variables. The service listens on port 8080.

| Variable | Meaning |
|---|---|
| `ACCOUNTS_BASE_URL` | Accounts API |
| `FX_BASE_URL` | FX API |
| `FRAUD_BASE_URL` | Fraud API |
| `CORE_BASE_URL` | Core banking, used by the SDK |
| `CORE_SDK_READ_TIMEOUT_MS` | SDK read timeout (default 2000) |
| `RECONCILIATION_INTERVAL_SECONDS` | Seconds between reconciliation passes (default 30) |
| `RECONCILIATION_GRACE_SECONDS` | How long a transfer stays PENDING before "not found" means FAILED (default 10) |
| `FRAUD_THRESHOLD_IDR`, `FRAUD_THRESHOLD_USD`, `FRAUD_THRESHOLD_SGD` | Fraud check thresholds |

Database settings are yours to name.
