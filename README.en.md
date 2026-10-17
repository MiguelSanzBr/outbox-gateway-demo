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

## Guarantees and tests

Integration tests against a real Postgres (`internal/outbox/`, `internal/api/`, `cmd/`):

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

100% statement coverage (excluding the `internal/testdb` helper), enforced in CI. Database error paths run against a real Postgres, no DB mocks: a closed pool simulates the database being down, and a trigger in the test schema breaks one specific statement mid-transaction.

To check the ordering tests don't pass by luck, I removed the ordering condition from the claim query: the first two tests in the table failed.

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
- No `LISTEN/NOTIFY` (polling) and no per-provider rate limit; a 429 is just a retry.
- The lease must be longer than the slowest send; the send is cut at half the lease. The clock used is Postgres's (`now()`), not the workers'.
- The mock keeps everything in memory and persists nothing.

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

`report.sh` waits for the outbox to drain and prints accepted, delivered, end-to-end latency and the mock counters (`duplicates`, `order_violations`). Mock variables: `MOCK_LATENCY`, `MOCK_JITTER`, `MOCK_FAIL_RATE`. Gateway: `WORKERS`, `LEASE`, `MAX_ATTEMPTS`, `BACKOFF_BASE`, `BACKOFF_MAX`.

Manual call:

```bash
curl -i -X POST localhost:8080/v1/messages \
  -H 'Idempotency-Key: order-42' \
  -d '{"recipient":"5511999990000","body":"Your order is out for delivery"}'
curl localhost:8080/v1/messages/1
```

## Layout

```
cmd/gateway          API + workers + reaper in one process
cmd/mockprovider     simulated provider
internal/outbox      schema, store (claim, lease, reaper), workers, HTTP sender
internal/api         HTTP handlers and validation
internal/mockprovider latency, failures, dedup and order checking
load/                k6 script, report and raw results
```
