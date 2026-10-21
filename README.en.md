# outbox-gateway-demo

[![ci](https://github.com/MiguelSanzBr/outbox-gateway-demo/actions/workflows/ci.yml/badge.svg)](https://github.com/MiguelSanzBr/outbox-gateway-demo/actions/workflows/ci.yml)
![Go 1.26](https://img.shields.io/badge/go-1.26-00ADD8)
![coverage 100% gated in CI](https://img.shields.io/badge/coverage-100%25%20(gated%20in%20CI)-brightgreen)
[![license MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

A message gateway in Go with a *transactional outbox* on PostgreSQL. Writing the message to the database and calling the provider don't fit in one transaction: if the process dies between the two, the message is lost or goes out twice.

Here the API writes the message and the intent to send it in the same transaction, and workers deliver to the provider **at least once**, in commit order per recipient, even with a `kill -9` in the middle of a send. The `Idempotency-Key` goes along so the provider can drop the resend; with that help from the provider, the effect is exactly-once delivery.

I built this repo because the WhatsApp gateway I built in production is private and I can't show the code. So I rewrote the same pattern from scratch, with new code, and used the chance to close the hole that was left there: this one has a reaper. The provider is an HTTP mock with configurable latency and failures.

[Versão em português](README.md)

![Live dashboard: per-state counters, workers with one killed, the reaper notice and the per-recipient lanes](docs/painel.png)

## How it works

```
POST /v1/messages ──► BEGIN; recipient lock; INSERT messages; INSERT outbox; COMMIT ──► 202
                        (pending if the recipient is free, queued if one is already ahead)
                                        │
             N workers ◄── claim the ready head: FOR UPDATE SKIP LOCKED + lease ──┘
                 │
                 ├─ send to the provider with Idempotency-Key
                 ├─ 2xx                         → sent, the recipient's next queued moves up
                 ├─ 408/425/429/5xx/network     → pending, backoff with jitter (Retry-After as a floor)
                 └─ other 4xx or out of attempts → dead, the next one moves up

             reaper: sending with an expired lease → pending with backoff
                     (on the last attempt: dead, with "uncertain outcome" as the reason)
```

### See it running

```bash
DEMO_ADMIN=1 docker compose up -d --build
```

Open http://localhost:5180, click **Enfileirar 20 mensagens** (enqueue 20 messages) and kill a worker in the middle of a send. The row stays in `sending`, the reaper returns it after the lease and another worker delivers it. Ports only bind to `127.0.0.1`. The dashboard UI is in Portuguese; states and worker ids are the same as in this README.

## Guarantees

1. **Idempotent ingest.** `POST /v1/messages` requires `Idempotency-Key`. The message and the outbox row are written in the same transaction. Repeating the key returns `200` with the same id; the same key with different content returns `409`.
2. **Claim without waiting.** Each worker claims a row with `SELECT ... FOR UPDATE SKIP LOCKED`, marks it `sending` and writes a `lease_until` and a fresh `lease_token`. A row locked by someone else is skipped, not waited on.
3. **Per-recipient order, the commit order.** Per recipient only one message is the head (`pending` or `sending`); the others wait in `queued` and move up one at a time when the head becomes `sent` or `dead`. Whoever decides that (the `Enqueue` and the promotion) goes through a `pg_advisory_xact_lock` on the recipient, so ids come out in commit order even with concurrent producers. A partial unique index enforces one head per recipient in the database. A `dead` message releases the recipient: I chose not to block the conversation forever.
4. **Lease, reaper and fencing.** If the worker dies, the row stays in `sending` until the lease expires; the reaper returns it with the same backoff as a failure. On the last attempt it becomes `dead` with "uncertain outcome" as the reason, because the provider may have received it before the worker died. Every write requires the current `lease_token`, so a worker that "wakes up" after losing its lease can't overwrite anything. The original gateway had no reaper; that's the improvement in this version.
5. **At least once, not exactly once.** The idempotency key goes in the header so the provider can drop resends. Without that help from the provider, a crash between the send and the commit produces a duplicate. There's no way to avoid that from the sender side alone.
6. **A deploy doesn't kill messages.** On `SIGTERM` the in-flight send finishes (it's only cut at half the lease) and the row is finalized; if the shutdown lands between the claim and the send, the row goes back to the queue without spending an attempt. In-flight HTTP requests finish before the pool closes.

## Architecture

Hexagonal, without ceremony: the domain in the middle, the use cases around it, and everything that talks to the outside world (Postgres, HTTP, the provider, SSE, the worker loop itself) is an adapter.

```
   driving adapters                                       driven adapters
   httpapi (REST) ──┐                                 ┌──► postgres (OutboxRepository, EventSource)
   sse ─────────────┼──► app (use cases) ───ports─────┼──► sse.Hub (EventSource: one LISTEN per process)
   worker (loop) ───┘            │                    ├──► provider (HTTP Sender)
                                 ▼                    └──► worker.Fleet (WorkerPool, demo only)
                              domain
```

- `internal/domain`: states and transitions (`queued → pending → sending → sent | dead | pending`), `OutboxEntry` with `Complete`, `Fail`, `Expire` (the reaper's decision) and `Release` (the shutdown), `LeaseToken` as the fencing token, `RetryPolicy` (backoff with jitter, attempt limit, `Retry-After` as a floor), message validation and the ordering rule on the receiving side. Plain Go, no JSON tags and no HTTP wording.
- `internal/app`: the use cases (`EnqueueMessage`, `ClaimNext`, `Deliver`, `ReapExpiredLeases`, `RequeueDead`, `StreamEvents`, `KillWorker`), the read models (`MessageState`, `Change`), the delivery counters and the ports they use (`OutboxRepository`, `Sender`, `EventSource`, `WorkerPool`).
- `internal/adapters`: `postgres`, `provider`, `httpapi`, `sse`, `worker` and the `mockprovider`.

The dependency rule is a test, not an agreement: `TestDependencyRule` takes each package's transitive closure with `go list -deps` and checks it against a list of what each one may use. Domain and use cases don't reach pgx, network, database or process packages, not even indirectly; each adapter has its own list (`httpapi` using `sse` is written there as a decision) and a new package without a rule fails. `TestDependencyRuleCatchesViolations` proves the rule catches indirect dependencies, subpackages and adapters knowing about each other. In the dashboard, ESLint's `no-restricted-imports` does the same.

**Why the concurrency invariants live in the postgres adapter.** The domain decides the what: which transition comes next, retry or dead, how long to wait, including for the reaper. The database guarantees the when. `FOR UPDATE SKIP LOCKED`, the recipient lock, the head unique index and the fencing `WHERE lease_token = $2` only hold if they are atomic with the write, and only Postgres sees all workers at once. Moving that into memory would mean read, decide and write in separate steps, with a race in between. For the same reason there's no `Clock` port: lease and reaper use Postgres' `now()` on purpose, so nothing depends on each worker's clock. States go as literals in the SQL (a partial index only matches constants) and `TestSQLStatusesMatchDomain` ties the literals to the domain's.

## Guarantees and tests

Integration tests against a real Postgres (`internal/adapters/`, `cmd/`) and unit tests for the domain, the use cases and the SSE hub:

| Guarantee | Test |
|---|---|
| No loss, no duplicates, order preserved with 40 concurrent producers, 8 workers and 20% provider failures | `TestDeliveryNoLossNoDuplicatesInOrder` |
| 10 producers at the same time on the same recipient: nothing stalls, nothing duplicates, the provider receives in commit order | `TestConcurrentProducersSameRecipient` |
| Only the recipient's head goes out; a head in backoff holds only its recipient; `dead` promotes the next; the database refuses two heads | `TestOnlyHeadOfRecipientIsClaimable`, `TestHeadInBackoffHoldsOnlyItsRecipient`, `TestDeadPromotesNextOfRecipient`, `TestDBRefusesTwoHeadsPerRecipient` |
| A row locked by another transaction is skipped, not waited on | `TestClaimSkipsLockedRows` |
| Worker killed after the provider received the message: the reaper returns it, another worker resends, the provider drops the resend and the recipient's next message only goes out afterwards | `TestReaperRecoversWorkerKilledMidSend` |
| Reaper follows the domain rule: backoff before the last attempt, `dead` with uncertain outcome on it | `TestReapAppliesDomainRule`, `TestExpire` |
| A worker with an expired lease can't finalize the row; the other attempt delivers | `TestStaleLeaseCannotFinish`, `TestWorkerThatLostLeaseDoesNotFinish` |
| `SIGTERM` with an in-flight send ends `sent` even on the last attempt; shutdown before the send releases without spending an attempt; the send is cut at half the lease | `TestFleetShutdownMidSendDelivers`, `TestShutdownDoesNotCutSend`, `TestShutdownBeforeSendReleases`, `TestDeliverDeadlines` |
| Shutdown waits for the in-flight HTTP request before closing the pool | `TestRunWaitsInFlightRequests` |
| 4 replicas booting together migrate without errors, each version once; a database from before versioned migrations boots | `TestMigrateConcurrentReplicas`, `TestMigrateUpgradesPreVersioningDB` |
| A 4xx goes to `dead` without retry; 408/425/429/5xx retry; `Retry-After` becomes a floor | `TestPermanentErrorGoesDead`, `TestSenderClassifiesResponses`, `TestRetryAfter` |
| Idempotent ingest (store and HTTP), including the same key with a different recipient | `TestEnqueueIsIdempotent`, `TestCreateMessage` |
| Invalid input is a client error: large body (413), unknown field, garbage after the JSON, NUL and invalid UTF-8 | `TestCreateMessage`, `TestNewMessage` |
| Bad config fails at boot, with all errors together | `TestLoadConfigFailsFast` |
| Message and outbox go in together or not at all; if the promotion fails, the head's `sent` doesn't stick either | `TestEnqueueIsAtomic`, `TestFinishRollsBackWhenPromoteFails` |
| Database down: the store returns the error, workers log with backoff and keep trying, the API answers 500 without leaking details, `/readyz` answers 503 | `TestStoreSurfacesDBErrors`, `TestWorkersSurviveDBOutage`, `TestClaimErrorsBackOff`, `TestReadEndpointsAndErrors`, `TestReadyz` |
| A single `LISTEN` per process, a stream cap with 503, a slow client dropped without blocking the others | `TestHubFansOutOneListen`, `TestHubCapsStreams`, `TestHubDropsSlowStream`, `TestEventsStream` |
| Every state change becomes an event, including the reaper's, with an increasing version | `TestListenStreamsStateChanges` |
| `/metrics` with queue by state, oldest age and deliveries by result; access log with request id | `TestMetrics`, `TestDeliveryCounters`, `TestAccessLogWithRequestID` |
| Redrive of `dead` and admin routes only with `DEMO_ADMIN=1` | `TestAdminRequeueDead`, `TestRequeueDead`, `TestAdminDisabledByDefault`, `TestRunDemoAdminFlagAndSSEShutdown` |
| Dependency rule between the layers | `TestDependencyRule`, `TestDependencyRuleCatchesViolations` |
| Dashboard: a late event doesn't move state backwards, a window of the newest 500, SSE reconnect with backoff | `board.test.ts`, `message.test.ts`, `sse.test.ts` (Vitest) |
| In the browser, against the real stack: a worker killed mid-send comes back through the reaper and is delivered; gateway stopped and started again, the dashboard goes back to "ao vivo" (live) by itself | `e2e/demo.spec.ts` (Playwright) |

100% statement coverage (except the `internal/testdb` helper), gated in CI. The dashboard has the same gate in Vitest (statements, branches, functions and lines), except `main.tsx`, which only wires the real adapters and is exercised by the E2E. Database error paths run against a real Postgres, no mocks: a closed pool to simulate the database being down and a trigger in the test schema to break one specific statement in the middle of the transaction. Each test uses its own schema, dropped at the end, so two runs on the same database don't step on each other.

Coverage is Go coverage, not SQL coverage: a whole `WHERE` counts as one line. So I tested 23 mutants by hand, changing one piece at a time and running the whole suite, and all of them died. The ones the review found surviving in the previous version, and what catches each one now:

| Mutant | Caught by |
|---|---|
| claim ignores `next_attempt_at` (backoff) | `TestHeadInBackoffHoldsOnlyItsRecipient`, `TestReapAppliesDomainRule` |
| `Finish` writes a zero backoff | `TestHeadInBackoffHoldsOnlyItsRecipient`, `TestReapAppliesDomainRule` |
| the ordering rule (promotion picks the newest, a head check that only looks at `sending`) | `TestDeadPromotesNextOfRecipient`, `TestConcurrentProducersSameRecipient`, `TestDBRefusesTwoHeadsPerRecipient` |
| reaper never sends to `dead` | `TestReapAppliesDomainRule`, `TestExpire` |
| send cut at `Lease*2` instead of `Lease/2` | `TestDeliverDeadlines` |
| send or finalization tied to the process ctx (the `SIGTERM`) | `TestShutdownDoesNotCutSend`, `TestFleetShutdownMidSendDelivers` |
| no `SKIP LOCKED` | `TestClaimSkipsLockedRows` |
| 429 treated as permanent | `TestSenderClassifiesResponses` |
| no head unique index | `TestDBRefusesTwoHeadsPerRecipient` |
| `sent_at` never written | `TestOnlyHeadOfRecipientIsClaimable` |
| key conflict ignores the recipient | `TestCreateMessage`, `TestEnqueueIsIdempotent` |
| no `MaxBytesReader` | `TestCreateMessage` |

The others: recipient lock turned into a no-op (`TestConcurrentProducersSameRecipient`), migration without the advisory lock (`TestMigrateConcurrentReplicas`), a hub that doesn't drop slow clients (`TestHubDropsSlowStream` hangs until the timeout), `Finish` without fencing, reaper ignoring `lease_until`, `Exhausted` ignored in `Fail`, release spending the attempt, `Retry-After` ignored, `dead` not promoting the next one and the claim out of readiness order.

## Load test numbers

Measured with k6 1.3.0, all on the same machine: AMD Ryzen 5 5600GT (6 cores/12 threads), ~11 GiB of RAM, Docker 29, Postgres 17 untuned. Mock: 80 ms + 0 to 120 ms of latency and 5% 503 responses. 10 s lease, reaper every 2 s, 1,000 recipients. Every run is from commit `f9b2b53`, with the NOTIFY trigger on, and came out of `load/run.sh`; the raw output, with the commit, the config and the load average before the run in the header, is in [`load/results/`](load/results/) (from `05` on; `01` to `04` are from the first version, before the trigger and the fixes).

| Scenario (60 s of load) | Accepted | No 202 (error or not fired by k6) | Delivered | Resends dropped by the provider | Order violations | End to end p50 / p95 / p99 |
|---|---:|---:|---:|---:|---:|---|
| 100 req/s, 16 workers | 6,000 | 0 | 6,000 | 0 | 0 | 163 / 248 / 530 ms |
| 300 req/s, 64 workers | 18,001 | 0 | 18,001 | 0 | 0 | 151 / 214 / 506 ms |
| 300 req/s, 64 workers, another run with the machine busier | 17,989 | 11 | 17,989 | 0 | 0 | 2,925 / 7,197 / 9,923 ms |
| 100 req/s, 16 workers, `kill -9` on the gateway at 20 s, back at 25 s | 5,593 | 22 + 387 | 5,593 | 14 | 0 | 601 / 1,805 / 3,429 ms |
| Same crash with 32 workers | 5,597 | 20 + 384 | 5,597 | 16 | 0 | 157 / 634 / 888 ms |

"End to end" goes from the write in the API to `sent` in the database, so it includes queueing, retries and the mock's latency. Retries caused by 503 show up in `load/results/` as "tentativas extras" (extra attempts).

What the numbers show:

- **No loss and no order violation in any run**, including with `kill -9`; `report.sh` exits with an error if either happens. In the 32-worker crash, 16 rows were in flight: they came back through the reaper after the lease and all 16 had already reached the provider, which dropped the resend by key. That's *at-least-once* showing up, with the provider's deduplication closing the gap.
- **A request without a 202 during the crash isn't a loss.** Without a 202 the client got no promise and resends with the same key. k6 doesn't resend, it only counts: connection errors while the gateway was down and iterations it never fired because the VUs were stuck.
- **16 workers leave little headroom.** Each send takes 80 + 60 ms on average (the jitter's mean), about 140 ms, so 16 workers give about 114 msg/s of capacity for 100 req/s of arrivals: ρ ≈ 0.88. With the queue working close to its limit, any disturbance takes long to drain, which is why the crash shows up in the p50 with 16 workers and nearly disappears with 32. In the first version of this README I said I hadn't looked into why the queue didn't drain; that's the math.
- **At 300 req/s the system is at its limit.** With the machine idle, delivery keeps up with arrivals (~298 msg/s, p95 of 214 ms). In a run with the machine busier, throughput dropped to 269 msg/s and the queue grew until the end of the test. The bottleneck is still Postgres: each message costs three commits (ingest, claim and finalization). Before the fixes this scenario gave 172 / 1,864 / 2,013 ms, 288 msg/s and 187 dropped iterations.
- **Claim with a large backlog.** With 100 thousand rows queued and the head of each of the 1,000 recipients in backoff (the provider down), the old claim did an anti-join per row: 99,000 probes, 398 thousand buffers and ~350 ms per claim just to find out there was nothing to do, with 16 workers repeating that every 50 ms. With only the head in `pending` it's an index range: 2 buffers and 0.06 ms. Both plans are in [`load/results/claim-100k-backlog.txt`](load/results/claim-100k-backlog.txt).
- **The machine isn't dedicated** (it's my work machine, running other things), so each file records the load average before the run. To tell noise from regression I ran the pre-fix code and the new code alternately, 100 req/s with 16 workers: before 572 / 1,517 / 1,639 and 165 / 323 / 537 ms, new 163 / 248 / 534 and 183 / 648 / 996 ms ([`ab-antes-vs-novo-100rps.txt`](load/results/ab-antes-vs-novo-100rps.txt)). The difference between two runs of the same code is larger than between the two versions.

## Trade-offs: Postgres as a queue vs. a broker

**For:** the message and the intent to send it are written atomically together with the business data, with no *dual write*; one less piece to operate; status queryable with SQL; per-recipient order expressed in the schema itself.

**Against:** three commits per message put the ceiling on WAL; workers poll (50 ms) instead of getting a push; the table grows and creates vacuum work; the recipient lock serializes producers of the same recipient.

**When to switch:** sustained load of thousands of messages per second, or several consumers of the same event. The outbox is still useful as a bridge (a *relay* publishes to Kafka/NATS/RabbitMQ), but it stops being the work queue.

## Known limits

- **Head-of-line blocking:** a message in backoff holds back the following ones for the same recipient. A `dead` message releases the recipient, and redriving it puts it back at the front of that recipient's queue, after newer messages have already gone out.
- The guaranteed order is the commit order. Concurrent requests for the same recipient go out in the order they committed, which isn't necessarily the order the client sent them.
- `sent` rows are never deleted. In production that calls for partitioning by date or archiving.
- Workers poll (`LISTEN/NOTIFY` only feeds the dashboard) and there's no per-provider rate limit; a 429 is a retry with `Retry-After` as a floor.
- The lease must be longer than the slowest send; the send is cut at half the lease. The clock in use is Postgres' (`now()`), not the workers'.
- The mock keeps everything in memory and persists nothing.

## Operations

- `GET /healthz` only says the process is up; `GET /readyz` pings the database and answers 503 without it, so the orchestrator takes traffic away.
- `GET /metrics` in the Prometheus text format, written by hand (it's half a dozen series): `outbox_messages{status}` without the `sent` ones, `outbox_oldest_unsent_seconds` (age of the oldest message not yet delivered, the SLI of an outbox), `outbox_db_up`, `outbox_deliveries_total{result="sent|retry|dead|lease_lost|released"}` and `outbox_reaped_total`. The queue comes from the database (the same on every replica, use `max`); the counters are per process (sum the replicas).
- JSON logs through `slog`, with one line per request (method, route, status, duration and `request_id`, taken from the client's `X-Request-Id` or a fresh random one, and echoed back in the header). Internal errors are logged with the same id.
- Config by environment, validated at boot: `WORKERS` (1 to 1024), `LEASE`, `POLL_INTERVAL`, `REAP_INTERVAL`, `MAX_ATTEMPTS`, `BACKOFF_BASE`, `BACKOFF_MAX` (up to 24 h), `SSE_MAX_STREAMS`, `PROVIDER_URL` (http or https), `DATABASE_URL`, `ADDR`, `DEMO_ADMIN`. A bad value doesn't silently fall back to the default: the process exits with all errors at once.
- Versioned migrations in `internal/adapters/postgres/migrations`, applied at boot in one transaction behind an advisory lock: replicas booting together wait for the first one.
- Redrive of a `dead` message (with `DEMO_ADMIN=1`): `curl -X POST localhost:8080/v1/admin/messages/42/requeue`. It comes back with zero attempts, as the head if the recipient is free or at the front of its queue.

## Live dashboard

`dashboard/` is a Vite, React 19 and strict TypeScript dashboard that shows messages moving between states live: per-recipient lanes (you can see the order being respected), per-state counters over the newest 500 messages, the worker list with a kill button and a notice when the reaper returns a row. It follows the same split as the Go code: `domain` (message state and counters), `application` (use cases over the `GatewayApi` and `GatewayEvents` ports) and `adapters` (fetch, `EventSource` and the React screen). The UI is in Portuguese.

How it works:

1. An `AFTER INSERT OR UPDATE` trigger on `outbox` does `pg_notify` with id, recipient, status, attempts, worker, version and last error. Since the reaper is just another `UPDATE`, its return shows up like any other change.
2. The gateway opens a single `LISTEN` per process (the `sse.Hub`) when the first stream arrives, and drops it when the last one leaves. Each `GET /v1/events` gets the changes through its own buffer; a client that doesn't read in time is dropped instead of holding the others back, and above `SSE_MAX_STREAMS` the stream gets a 503. A ping every 15 s keeps proxies from dropping an idle connection; shutting the gateway down closes every stream.
3. The dashboard fetches the snapshot from `GET /v1/messages` (the newest 500) with TanStack Query and applies the `EventSource` events on top, keeping only the newest 500. Every time the SSE connects or reconnects it fetches the snapshot again, because NOTIFY keeps nothing. If the response isn't a 200 (nginx's 502 with the gateway down, 503 above the cap) the `EventSource` gives up for good; then the adapter itself opens another one, with a 1 to 30 s backoff and jitter.
4. Snapshot and events arrive in any order. A `BEFORE UPDATE` trigger adds 1 to the `version` column on every change, and the dashboard only applies what has a higher version than what it already has. I used to order by `attempts * 2 + phase`, with no new column, but the shutdown release and the redrive move `attempts` back, and then the math no longer only grows.

**Why LISTEN/NOTIFY and not polling.** Polling on `updated_at` or a version column loses events: two transactions can write in one order and commit in the other, and the cursor skips the one that committed later. NOTIFY only fires on commit, in commit order, and catches any writer (worker, reaper, even a manual `UPDATE`). The cost:

- It isn't durable: whoever wasn't listening loses the event. That's why the snapshot is reloaded on every reconnect.
- Every commit that notifies goes through a global lock on the notification queue. The load numbers above already include the trigger.
- Payload of at most 8000 bytes, which is why `last_error` is cut to 200 characters.

**Kill a worker for real.** With `DEMO_ADMIN=1` the gateway enables `GET /v1/admin/workers`, `POST /v1/admin/workers/{id}/kill`, `POST /v1/admin/workers/revive` and the redrive. Kill cancels that worker's context with its own cause: if it's sending, the send is cut and the row isn't finalized. It stays in `sending` with that worker's id until the lease expires and the reaper returns it, and in the dashboard you can see the row stuck, the "devolvida pelo reaper" (returned by the reaper) notice and another worker delivering it. The "Enfileirar 20 mensagens" button sends a burst to 4 recipients through the API itself, one request at a time so the commit order is the send order.

These routes are off by default and **must never be turned on in production**: they have no authentication. In the compose file the ports only bind to `127.0.0.1`, so not even the local network reaches them. Note that `GET /v1/events` and `GET /v1/messages` show the recipient of every message, like `GET /v1/messages/{id}`; in a real gateway all of that sits behind authentication.

Accessibility: the notices (reaper, dead, actions) live in a `role="log"` with `aria-live`, without announcing every `sent` so the screen reader isn't flooded; each message has an accessible name with state, attempt and worker; state is never color alone; everything is a native button and works with the keyboard; light and dark themes via `prefers-color-scheme`; the grid fits in 375 px.

## How to run

Tests (they need Postgres; without `DATABASE_URL` they skip, except in CI, where they fail):

```bash
docker compose up -d postgres
DATABASE_URL=postgres://outbox:outbox@localhost:5432/outbox go test -race ./...
```

With coverage (the same command as CI):

```bash
DATABASE_URL=postgres://outbox:outbox@localhost:5432/outbox \
  go test -race -coverpkg=$(go list ./... | grep -v /internal/testdb | paste -sd,) -coverprofile=cover.out ./...
go tool cover -func=cover.out | tail -1
```

Lint and vulnerabilities, as in CI:

```bash
golangci-lint run ./...
go run golang.org/x/vuln/cmd/govulncheck@v1.5.0 ./...
```

Load test: `load/run.sh` brings the stack up from scratch, runs k6, waits for the queue to drain and writes the result to `load/results/` with the commit and the config in the header. It exits with 1 if a message is lost or the order is broken. With `KILL_AT` it sends `SIGKILL` to the gateway and starts it again 5 s later.

```bash
RATE=100 WORKERS=16 ./load/run.sh 05-100rps-16workers
RATE=100 WORKERS=16 KILL_AT=20 ./load/run.sh 07-100rps-kill9-gateway-16workers
```

Mock variables: `MOCK_LATENCY`, `MOCK_JITTER`, `MOCK_FAIL_RATE`. For Postgres in compose: `PG_PORT`, if 5432 is already taken.

Manual call:

```bash
curl -i -X POST localhost:8080/v1/messages \
  -H 'Idempotency-Key: order-42' \
  -d '{"recipient":"5511999990000","body":"Your order is out for delivery"}'
curl localhost:8080/v1/messages/1
```

Dashboard in dev mode, with the gateway running on `:8080`:

```bash
cd dashboard && npm ci
npm run dev        # Vite on :5180, proxies /v1 to the gateway
npm run lint && npm run typecheck && npm run coverage && npm run build
```

E2E with Playwright against the real stack. With 2 workers and 1 s per send there's always a worker in the middle of a send to kill:

```bash
docker compose down -v
DEMO_ADMIN=1 WORKERS=2 LEASE=6s MOCK_LATENCY=1s MOCK_JITTER=0s MOCK_FAIL_RATE=0 docker compose up -d --build
cd dashboard && npx playwright install chromium && npm run e2e
```

## Layout

```
cmd/gateway                    composition root and config: API + workers + reaper in one process
cmd/mockprovider               simulated provider
internal/domain                states, transitions, lease, retry, validation and ordering (plain Go)
internal/app                   use cases, ports, read models and counters
internal/adapters/postgres     migrations, repository (claim, lease, promotion, reaper) and LISTEN/NOTIFY
internal/adapters/httpapi      HTTP handlers, /readyz, /metrics and access log
internal/adapters/sse          one-LISTEN-per-process hub and the SSE stream for the dashboard
internal/adapters/provider     HTTP sender to the provider
internal/adapters/worker       worker and reaper loops, demo kill and revive
internal/adapters/mockprovider latency, failures, deduplication and order checking
internal/archtest              dependency rule between the layers
dashboard/                     dashboard (domain, application, adapters) and E2E
load/                          k6 script, run.sh, report and raw results
docs/                          dashboard screenshot
```
