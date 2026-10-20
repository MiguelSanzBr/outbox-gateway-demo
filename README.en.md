# outbox-gateway-demo

Message gateway in Go with a *transactional outbox* on PostgreSQL: it takes send requests over HTTP and delivers them to a slow, flaky external provider without losing a message, without delivering twice and without reordering messages for the same recipient, even if the process dies mid-send.

I built this because the WhatsApp gateway I wrote in production is private and I can't show the code. So I rewrote the same pattern from scratch, with new code, and closed the gap I left there: this one has a reaper. The provider is an HTTP mock with configurable latency and failures.

[Versão em português](README.md)

## How it works

```
POST /v1/messages ──► BEGIN; INSERT messages; INSERT outbox; COMMIT ──► 202
                                        │
             N workers ◄── claim: FOR UPDATE SKIP LOCKED + lease ──┘
                 │
                 ├─ sends to the provider with Idempotency-Key
                 ├─ 2xx            → sent
                 ├─ 5xx/429/timeout → pending, exponential backoff with jitter
                 └─ other 4xx      → dead

             reaper: sending with expired lease → pending
```

1. **Idempotent intake.** `POST /v1/messages` requires `Idempotency-Key`. The message and the outbox row are written in the same transaction. Repeating the key returns `200` with the same id; the same key with different content returns `409`.
2. **Claim.** Each worker claims a row with `SELECT ... FOR UPDATE SKIP LOCKED`, sets it to `sending` and writes a `lease_until` and a fresh `lease_token`. Workers never wait on each other.
3. **Per-recipient order.** The claim only takes the oldest message of a recipient that has no earlier message still open (`NOT EXISTS`). A partial unique index (`recipient WHERE status = 'sending'`) enforces at most one in-flight message per recipient in the database itself.
4. **Lease, reaper and fencing.** If the worker dies, the row stays `sending` until the lease expires; the reaper puts it back in the queue. Every finalization requires the current `lease_token`, so a worker that "wakes up" after losing its lease can't overwrite the state. The original gateway had no reaper; that's the improvement in this version.
5. **Effectively-once delivery.** The gateway guarantees *at least once*; the idempotency key goes in the header so the provider can drop resends. Without that support from the provider, a crash between the send and the commit produces a duplicate. There's no way around that from the sender side alone.

## Architecture

Hexagonal, without ceremony: the domain in the middle, use cases around it, and everything that talks to the outside world (Postgres, HTTP, the provider, SSE, the worker loop itself) is an adapter.

```
   driving adapters                                       driven adapters
   httpapi (REST) ──┐                                 ┌──► postgres (OutboxRepository, EventSource)
   sse ─────────────┼──► app (use cases) ───ports─────┼──► provider (HTTP Sender)
   worker (loop) ───┘            │                    └──► worker.Fleet (WorkerPool, demo only)
                                 ▼
                              domain
```

- `internal/domain`: states and transitions (`pending → sending → sent | dead | pending`), `OutboxEntry` with `Complete` and `Fail`, `LeaseToken` as the fencing token, `RetryPolicy` (backoff with jitter and attempt limit), message validation and the per-recipient ordering rule. Plain Go.
- `internal/app`: the use cases (`EnqueueMessage`, `ClaimNext`, `Deliver`, `CompleteDelivery`, `FailDelivery`, `ReapExpiredLeases`, `StreamEvents`, `KillWorker`) and the ports they use (`OutboxRepository`, `Sender`, `EventSource`, `WorkerPool`).
- `internal/adapters`: `postgres`, `provider`, `httpapi`, `sse`, `worker` and the `mockprovider`.

The dependency rule is a test, not a convention: `TestDependencyRule` reads the imports of `domain` and `app` with `go/parser` and fails on pgx, `net/http` or any adapter. In the dashboard ESLint's `no-restricted-imports` does the same.

**Why the concurrency invariants live in the postgres adapter.** The domain decides the what: which transition comes next, retry or dead, how long to wait. The database guarantees the when. `FOR UPDATE SKIP LOCKED`, the `NOT EXISTS` for per-recipient order, the partial unique index for one in-flight message per recipient and the `WHERE lease_token = $2` fencing only hold if they are atomic with the write, and only Postgres sees every worker at once. Pulling that into memory would turn it into read, decide and write as separate steps with a race in between. That's why `domain.RecipientOrder` exists to check order on the receiving side (the mock uses it), while the guarantee itself stays in the claim SQL. Same reason there is no `Clock` port: lease and reaper use Postgres's `now()` on purpose, so nothing depends on each worker's clock.

## Guarantees and tests

Integration tests against a real Postgres (`internal/adapters/`, `cmd/`) and unit tests for the domain and use cases:

| Guarantee | Test |
|---|---|
| No loss, no duplicates, order preserved with 40 concurrent producers, 8 workers and 20% provider failures | `TestDeliveryNoLossNoDuplicatesInOrder` |
| Worker killed after the provider received: reaper returns it, another worker resends, provider drops the resend and the recipient's next message only goes out after | `TestReaperRecoversWorkerKilledMidSend` |
| Worker with an expired lease can't finalize the row | `TestStaleLeaseCannotFinish` |
| 4xx goes to `dead` with no retry | `TestPermanentErrorGoesDead` |
| Idempotent intake (store and HTTP) | `TestEnqueueIsIdempotent`, `TestCreateMessage` |
| Message and outbox row go in together or not at all: a failure in either insert leaves nothing half-written | `TestEnqueueIsAtomic` |
| Worker that took longer than its lease doesn't mark `sent` with a stale token; the next attempt delivers | `TestWorkerThatLostLeaseDoesNotFinish` |
| Database down: store returns the error, workers log and keep retrying, API answers 500 without leaking details | `TestStoreSurfacesDBErrors`, `TestWorkersSurviveDBOutage`, `TestReadEndpointsAndErrors` |
| `SIGINT` shuts gateway and mock down cleanly; if listen fails, workers stop before the pool closes | `TestMainGracefulShutdown`, `TestRunListenErrorStopsWorkers` |
| Every state change becomes an event on `LISTEN`, including the reaper putting a row back | `TestListenStreamsStateChanges` |
| SSE with heartbeat; a client that leaves releases its `LISTEN` connection; shutting the gateway down closes the stream | `TestEventsStream`, `TestRunDemoAdminFlagAndSSEShutdown` |
| A worker killed mid-send doesn't finalize: the row stays `sending` until the reaper and, once revived, it delivers again | `TestFleetKillMidSendLeavesRowForReaper` |
| Admin routes only exist with `DEMO_ADMIN=1` | `TestAdminDisabledByDefault`, `TestRunDemoAdminFlagAndSSEShutdown` |
| State machine and the retry or dead decision in the domain, no database | `TestStatusMachine`, `TestEntryTransitions` |
| Domain and use cases don't import pgx, `net/http` or adapters | `TestDependencyRule` |
| Dashboard: a late event never moves the state backwards; reaper and dead become notices | `board.test.ts`, `message.test.ts` (Vitest) |
| In the browser, against the real stack: an enqueued message reaches `sent`; a worker killed mid-send gets its row back through the reaper and it is delivered | `e2e/demo.spec.ts` (Playwright) |

100% statement coverage (excluding the `internal/testdb` helper), enforced in CI. The dashboard has the same gate in Vitest (statements, branches, functions and lines), excluding `main.tsx`, which only wires the real adapters and is exercised by the E2E. Database error paths run against a real Postgres, no DB mocks: a closed pool simulates the database being down, and a trigger in the test schema breaks one specific statement mid-transaction.

To check the ordering tests don't pass by luck, I removed the ordering condition from the claim query: the first two tests in the table failed.

I did the same for the dashboard and the admin routes, breaking things on purpose and restoring them: turning the admin routes on without checking `DEMO_ADMIN` fails `TestRunDemoAdminFlagAndSSEShutdown`; a `WHEN` on the trigger that skips the return to `pending` leaves `TestListenStreamsStateChanges` without the reaper's event; letting a killed worker finalize the row fails `TestFleetKillMidSendLeavesRowForReaper`; accepting a late event in the dashboard fails two Vitest tests. Importing `net/http` in `app` or `app` in `domain` fails `TestDependencyRule`.

## Load test numbers

Measured with k6 on this setup, all on the same machine: AMD Ryzen 5 5600GT (6 cores/12 threads), ~11 GiB RAM, Docker 29, untuned Postgres 17. Mock: 80 ms + 0–120 ms latency and 5% 503 responses. 10 s lease, reaper every 2 s, 1,000 recipients. Raw output for every run is in [`load/results/`](load/results/).

| Scenario (60 s of load) | Accepted | Delivered | Resends dropped by provider | Order violations | End-to-end p50 / p95 / p99 |
|---|---:|---:|---:|---:|---|
| 100 req/s, 16 workers | 6,001 | 6,001 | 0 | 0 | 162 / 246 / 519 ms |
| 300 req/s, 64 workers | 17,814 | 17,814 | 0 | 0 | 172 / 1,864 / 2,013 ms |
| 100 req/s, 32 workers, `kill -9` on the gateway at 20 s, restarted at 25 s | 5,590 | 5,590 | 19 | 0 | 151 / 325 / 523 ms |
| Same crash with 16 workers | 5,599 | 5,599 | 14 | 0 | 2,718 / 4,137 / 4,337 ms |

"End-to-end" goes from the API write to `sent` in the database, so it includes queueing, retries and the mock's latency. Retries caused by 503 show up in `load/results/` as "tentativas extras" (extra attempts).

What the numbers show:

- **No loss and no order violation in any run**, including `kill -9`. In the 32-worker crash, 21 rows were in flight: the reaper returned them after the lease and 19 of them had already reached the provider, which dropped the resend by key. Those messages waited up to ~11.5 s (lease + reaper interval).
- **With 16 workers, the crash hurt the rest of the run**: latency went up to ~4 s after the restart and didn't recover before the end. With 32 workers the same crash doesn't show in p50. My read is that 16 workers × ~140 ms per send leave little headroom over 100 req/s, but I didn't dig into why the queue didn't drain.
- **At 300 req/s Postgres became the bottleneck:** API p95 went from ~2 ms to 111 ms, k6 dropped 187 iterations and delivery throughput settled at 288 msg/s. Each message costs three commits (intake, claim and finalization); in a separate test draining a backlog with 64 workers, the waits in `pg_stat_activity` were WAL (`WalSync`, `WALWrite`).

## Trade-offs: Postgres as a queue vs. a broker

**For:** the message and the intent to send it are written atomically with the business data, no *dual write*; one less thing to operate; status is queryable with SQL; per-recipient order fits in one query.

**Against:** three commits per message put the ceiling on WAL; workers poll (50 ms) instead of getting a push; the table grows and creates vacuum work; the claim query gets more expensive when many recipients have a blocked queue.

**When to switch:** sustained load of thousands of messages per second, or several consumers of the same event. The outbox stays useful as a bridge (a *relay* publishes to Kafka/NATS/RabbitMQ), but stops being the work queue.

## Known limits

- **Head-of-line blocking:** a message in backoff holds the next ones for the same recipient. A `dead` message releases the recipient: I chose not to block the conversation forever.
- The guaranteed order is commit order. Concurrent requests for the same recipient have no defined order.
- `sent` rows are never deleted. In production that calls for date partitioning or archiving.
- Workers poll (`LISTEN/NOTIFY` only feeds the dashboard) and there is no per-provider rate limit; a 429 is just a retry.
- The lease must be longer than the slowest send; the send is cut at half the lease. The clock used is Postgres's (`now()`), not the workers'.
- The mock keeps everything in memory and persists nothing.

## Realtime dashboard

`dashboard/` is a Vite, React 19 and strict TypeScript dashboard that shows messages moving across states live: lanes per recipient (you can watch the order being kept), counters per state, the worker list with a kill button, and a notice when the reaper puts a row back. It follows the same split as the Go side: `domain` (message state and counters), `application` (use cases over the `GatewayApi` and `GatewayEvents` ports) and `adapters` (fetch, `EventSource` and the React UI).

How it works:

1. An `AFTER INSERT OR UPDATE` trigger on `outbox` calls `pg_notify` with id, recipient, status, attempts, worker and last error. The reaper is just another `UPDATE`, so its change shows up like any other.
2. `GET /v1/events` opens its own connection with `LISTEN` and forwards each notification as an SSE event, with a heartbeat every 15 s so proxies don't drop the idle connection. A client that leaves releases the `LISTEN` connection; shutting the gateway down closes every stream.
3. The dashboard loads a snapshot from `GET /v1/messages` (the newest 500) with TanStack Query and applies the `EventSource` events on top. Every time the SSE connects or reconnects it reloads the snapshot, because NOTIFY keeps nothing.
4. Snapshot and events arrive in any order. Instead of a version column, the dashboard orders by lifecycle: each claim adds 1 to `attempts` and `sending` comes before the outcome of the same attempt, so `attempts * 2 + (sending ? 0 : 1)` only grows. An event at the same or an earlier position is ignored.

**Why LISTEN/NOTIFY and not polling.** Polling by `updated_at` or a version column loses events: two transactions can write in one order and commit in the other, and the cursor skips the one that committed later. NOTIFY only fires on commit, in commit order, catches any writer (worker, reaper, even a manual `UPDATE`) and needs no new column. The cost:

- It isn't durable: whoever wasn't listening misses the event. That's why the snapshot is reloaded on every reconnect.
- Every commit that notifies goes through a global lock on the notification queue. The load test numbers above were measured before the trigger existed and I haven't measured again; under heavy load it's worth dropping the trigger or switching the dashboard to polling.
- Payload is capped at 8000 bytes, so `last_error` is cut to 200 characters.
- Each SSE client holds a database connection. Fine for a demo; for many clients the right move is a single listener per process fanning out to the streams.

**Kill a worker for real.** With `DEMO_ADMIN=1` the gateway turns on `GET /v1/admin/workers`, `POST /v1/admin/workers/{id}/kill` and `POST /v1/admin/workers/revive`. Kill cancels that worker's context with its own cause: if it is sending, the send is cut and the row is not finalized. It stays `sending` with the worker id until the lease expires and the reaper puts it back, and on the dashboard you can see the row stuck, the "devolvida pelo reaper" notice and another worker delivering it. The "Enfileirar 20 mensagens" button sends a burst to 4 recipients through the regular API, one request at a time so commit order is send order.

These routes are off by default and **must never be turned on in production**: they have no authentication, so anyone on the network can kill the workers. Also, `GET /v1/events` and `GET /v1/messages` expose every message's recipient, same as the existing `GET /v1/messages/{id}`; in a real gateway all of that sits behind authentication.

Accessibility: notices (reaper, dead, actions) go to a `role="log"` with `aria-live`, without announcing every `sent` so screen readers don't drown; each message has an accessible name with state, attempt and worker; state is never color alone; everything is a native button and works from the keyboard; light and dark theme via `prefers-color-scheme`; the grid fits 375 px.

## Running it

Tests (need Postgres):

```bash
docker compose up -d postgres
DATABASE_URL=postgres://outbox:outbox@localhost:5432/outbox go test -race ./...
```

With coverage (same command as CI):

```bash
DATABASE_URL=postgres://outbox:outbox@localhost:5432/outbox \
  go test -race -coverpkg=$(go list ./... | grep -v /internal/testdb | paste -sd,) -coverprofile=cover.out ./...
go tool cover -func=cover.out | tail -1
```

Full stack and load test:

```bash
docker compose down -v && WORKERS=16 docker compose up -d --build
RATE=100 DURATION=60s docker compose run --rm k6
./load/report.sh
```

`report.sh` waits for the outbox to drain and prints accepted, delivered, end-to-end latency and the mock counters (`duplicates`, `order_violations`). Mock variables: `MOCK_LATENCY`, `MOCK_JITTER`, `MOCK_FAIL_RATE`. Gateway: `WORKERS`, `LEASE`, `MAX_ATTEMPTS`, `BACKOFF_BASE`, `BACKOFF_MAX`, `DEMO_ADMIN`.

Manual call:

```bash
curl -i -X POST localhost:8080/v1/messages \
  -H 'Idempotency-Key: order-42' \
  -d '{"recipient":"5511999990000","body":"Your order is out for delivery"}'
curl localhost:8080/v1/messages/1
```

Dashboard (comes up with compose, at http://localhost:5180):

```bash
DEMO_ADMIN=1 docker compose up -d --build
```

Without `DEMO_ADMIN=1` the dashboard works the same, just without the kill buttons. To work on the dashboard with the gateway running on `:8080`:

```bash
cd dashboard && npm ci
npm run dev        # Vite on :5180, proxies /v1 to the gateway
npm run lint && npm run typecheck && npm run coverage && npm run build
```

Playwright E2E against the real stack. With 2 workers and 1 s per send there is always a worker mid-send to kill:

```bash
docker compose down -v
DEMO_ADMIN=1 WORKERS=2 LEASE=6s MOCK_LATENCY=1s MOCK_JITTER=0s MOCK_FAIL_RATE=0 docker compose up -d --build
cd dashboard && npx playwright install chromium && npm run e2e
```

## Layout

```
cmd/gateway                    composition root: API + workers + reaper in one process
cmd/mockprovider               simulated provider
internal/domain                states, transitions, lease, retry, validation and order (plain Go)
internal/app                   use cases and ports
internal/adapters/postgres     schema, repository (claim, lease, reaper) and LISTEN/NOTIFY
internal/adapters/httpapi      HTTP handlers
internal/adapters/sse          SSE stream for the dashboard
internal/adapters/provider     HTTP sender to the provider
internal/adapters/worker       worker and reaper loops, demo kill and revive
internal/adapters/mockprovider latency, failures, dedup and order checking
internal/archtest              dependency rule between layers
dashboard/                     dashboard (domain, application, adapters) and E2E
load/                          k6 script, report and raw results
```
