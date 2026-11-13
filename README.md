# OnceOnly — Idempotent Event Processing System

### Send it twice. Process it once. Sleep at night.

Networks retry. Producers double-fire. Relays crash *after* publishing but *before* marking. Kafka, like every honest broker, delivers **at-least-once** — which means your consumer **will** see the same event twice, and in a payment system one duplicate is a customer charged twice.

"Exactly-once delivery" is marketing. What actually saves you is an **idempotent consumer**, and OnceOnly is a complete, runnable demonstration of every layer of that defence — with money on the line so correctness is *provable*: a mini payment ledger where the total money in the system must never change, and no transfer may ever apply twice. A built-in **chaos generator** spends its life trying to break that invariant. It can't.

Built with **Go**, **Kafka** (franz-go), **PostgreSQL 17**, and **Valkey**.

---

## Architecture

```
                ┌───────────────────────────── onceonly api ─────────────────────────────┐
                │  POST /transfers                                                     │
 Producer ──────┤    Idempotency-Key header ──► [1] API IDEMPOTENCY                    │
                │         │                                                            │
                │         └── ONE Postgres transaction: transfer row + OUTBOX event    │
                └──────────────────────────────┬─────────────────────────────────────────┘
                                               │  [2] TRANSACTIONAL OUTBOX
                                  onceonly relay (FOR UPDATE SKIP LOCKED,
                                  publish → mark, at-least-once by design)
                                               │
                                               ▼
                                  Kafka topic "transfers"
                                  (key = from_account → per-account ordering)
                                               │
                                               ▼
                ┌──────────────────────────── onceonly consumer ─────────────────────────┐
                │  [3] DEDUP GATE                                                        │
                │    a. Valkey fast path — "processed:{event_id}", written ONLY          │
                │       after the Postgres commit (a hit is trustworthy)                 │
                │    b. Postgres: INSERT INTO processed_events in the SAME               │
                │       transaction that moves the money. Unique conflict ⇒ duplicate.   │
                │  [4] VALIDATION + RETRY: invalid → DLQ now; transient → 3 attempts     │
                │       with backoff → DLQ; insufficient funds → DLQ (atomic)            │
                └──────────────────────────────┬─────────────────────────────────────────┘
                                               ▼
                       DLQ (table + "transfers-dlq" topic) ──► POST /dlq/{id}/replay
                                                                 (fix, republish, dedup
                                                                  makes replay safe)

   Observability: Prometheus metrics on every process → Grafana dashboard
   (received vs duplicates dropped vs applied vs dead-lettered, live).
```

One Go binary, subcommands per role: `api` · `relay` · `consumer` · `seed` · `chaos`.

---

## The four layers of defence

**1. Idempotency keys at the API (Stripe-style).** `POST /transfers` requires an `Idempotency-Key` header. First submission → `201` + the transfer. A retry with the same key and the same payload → `200`, the *original* transfer, `X-Idempotent-Replay: true`, and nothing new is created. Same key, *different* payload → `409 Conflict` (detected via a SHA-256 fingerprint of the request). A producer that retries an HTTP timeout can never create two transfers.

**2. The transactional outbox.** The transfer row and its event are written in **one** Postgres transaction. There is no dual-write window where the DB says "created" but Kafka never hears about it (or vice versa). The relay polls unpublished rows with `FOR UPDATE SKIP LOCKED`, publishes, then marks — inside one DB transaction. If it crashes between publish and mark, the event is published *again*. That's not a bug; it's the honest at-least-once relay, and it's why layer 3 exists.

**3. The dedup gate (DB is the source of truth).** For each event, the consumer opens one Postgres transaction that inserts a `processed_events` row **and** moves the money **and** updates the transfer status. The insert and the money commit or roll back *together* — there is no interleaving in which an event is marked processed but not applied, or applied but not marked. If the insert hits the unique constraint, this delivery is a duplicate: roll back, count it, ack it, move on. Valkey sits in front purely as a fast path ("have I definitely seen this?"), with keys written only *after* commit — a naive `SETNX`-before-processing would turn a mid-processing crash into silent data loss, which is why this design doesn't do that. Redis can be flushed, down, or lying; correctness doesn't care.

**4. Dead letters with a way back.** Permanent failures (invalid payload, insufficient funds) go straight to the DLQ. Transient failures retry 3 times with backoff, then DLQ. Every DLQ entry is visible via `GET /dlq` and replayable via `POST /dlq/{event_id}/replay`, which removes the processed marker, applies the operator's fix, and republishes with the **original event_id** — replay can't double-apply, because the same dedup machinery guards it.

> **Why this composition works:** at-least-once delivery (Kafka + outbox relay) guarantees nothing is *lost*; the idempotent consumer guarantees nothing is *applied twice*. Together: **effectively-once processing**, which is the strongest guarantee real systems actually ship.

---

## The Chaos Simulator 🔥

`onceonly chaos <mode> [count]`, or `POST /chaos/{mode}`. Two honestly-distinguished entry points: transfers are created **through the ingest path** (idempotency keys, outbox, relay — the real thing), while extra copies are **raw-published** straight to the topic with pre-built events, simulating blind producer retries and crash-republishing relays. The duplicates are real duplicates: same `event_id`, same payload.

| Mode | Attack | Expected result |
|---|---|---|
| `duplicate-storm` | One valid transfer event delivered **100 extra times** | Applied exactly once; ~100 duplicates dropped |
| `shuffle` | 20 transfers raw-published **out of order** (racing the relay's in-order copies) | All applied once; final balances identical to in-order application |
| `poison` | Transfers whose `reference` is `POISON` — the processor's simulated downstream fails on every attempt | 3 retries → DLQ → replay applies the fix (`POISON` → `POISON-FIXED`) → succeeds |
| `hurricane` | 25 transfers, each delivered **3–5×, interleaved and shuffled** | 25 applications, total money unchanged |

Why doesn't the shuffle break balances? Because balance effects *commute* — a credit and a debit land in any order and the totals agree. Ordering per account key still matters for the **overdraft check**: "was the balance sufficient *at that moment*?" depends on sequence, which is why events are partitioned by `from_account` (and why the README is honest that with shuffled delivery, an individual transfer's insufficient-funds verdict can legitimately depend on arrival order — the invariant that *never* breaks is the total).

---

## Quickstart

```bash
docker compose up --build        # Kafka, Postgres, Valkey, api, relay, consumer, Prometheus, Grafana
```

Seed the demo accounts (6 accounts, **139,150,000 cents** total — the invariant):

```bash
docker compose exec api onceonly seed
```

Make a transfer (note the idempotency key):

```bash
FROM=$(curl -s localhost:8080/accounts | python3 -c 'import json,sys; print(json.load(sys.stdin)["accounts"][0]["id"])')
TO=$(curl -s localhost:8080/accounts | python3 -c 'import json,sys; print(json.load(sys.stdin)["accounts"][1]["id"])')

curl -s -X POST localhost:8080/transfers \
  -H 'Idempotency-Key: rent-payment-001' \
  -H 'Content-Type: application/json' \
  -d "{\"from_account\":\"$FROM\",\"to_account\":\"$TO\",\"amount_cents\":5000,\"reference\":\"rent\"}"
# → 201 {"id":"…","status":"pending",…}

# Retry the EXACT same request (simulating a client timeout retry):
# → 200, same transfer id, header X-Idempotent-Replay: true, nothing new created.
```

Attack it, then watch the scoreboard:

```bash
docker compose exec api onceonly chaos duplicate-storm 100
curl -s localhost:8080/stats
# expected shape (exact numbers depend on what else you've run):
# {"events_received":101,"duplicates_dropped":100,"transfers_applied":1,
#  "dlq_count":0,"outbox_pending":0}

docker compose exec api onceonly chaos poison
curl -s localhost:8080/dlq                       # the poisoned event, attempts: 3
EVENT_ID=$(curl -s localhost:8080/dlq | python3 -c 'import json,sys; print(json.load(sys.stdin)["entries"][0]["event_id"])')
curl -s -X POST localhost:8080/dlq/$EVENT_ID/replay   # fix applied, republished…
curl -s localhost:8080/stats                          # …and applied exactly once
```

Grafana: <http://localhost:3000> (admin/admin) → dashboard **“OnceOnly — Idempotent Event Processing”**. Prometheus: <http://localhost:9090>.

Running without Docker: install Postgres 17, Kafka, and Valkey/Redis locally, copy `.env.example` to `.env` (defaults already point at localhost), then run `go run ./cmd/onceonly seed|api|relay|consumer` in separate terminals.

---

## API reference

| Endpoint | Description |
|---|---|
| `POST /transfers` | Create a transfer. **Requires `Idempotency-Key`.** `201` new · `200` + `X-Idempotent-Replay: true` on key+payload replay · `409` same key, different payload · `400` missing key / invalid body |
| `GET /transfers/{id}` | One transfer, including its `event_id` |
| `GET /accounts` | All accounts + `total_balance_cents` (the invariant) |
| `GET /accounts/{id}` | One account + its 20 most recent transfers |
| `GET /stats` | `events_received`, `duplicates_dropped`, `transfers_applied`, `dlq_count`, `outbox_pending` |
| `GET /dlq` | Dead-lettered events with error and attempt counts |
| `POST /dlq/{event_id}/replay` | Fix (poison reference), republish with the original id, mark replayed. `409` if already replayed, `404` if unknown |
| `POST /chaos/{mode}` | `duplicate-storm` · `shuffle` · `poison` · `hurricane`, JSON body `{"count": n}` |
| `GET /metrics` | Prometheus metrics |
| `GET /healthz` | Liveness |

Transfer request body: `{"from_account","to_account","amount_cents","reference"}`. Event payload on the topic: `{"event_id","transfer_id","type":"TransferRequested","from_account","to_account","amount_cents","reference","occurred_at"}`. Money is **integer cents** everywhere — floats and money are not friends.

---

## Design decisions & honest trade-offs

- **Postgres is the dedup source of truth, Valkey is a cache.** The exactly-once step is a single DB transaction (marker + money). This costs a DB round-trip per event; the Valkey fast path absorbs the common duplicate (producer retry storms arrive milliseconds apart) without ever being able to cause a false drop. If Valkey is down, the system is *slower*, never *wrong*.
- **Retries are in-process with backoff** (3 attempts: ~100ms, ~300ms, ~900ms) and only for *transient* failures. Permanent failures (invalid payload, unknown account, insufficient funds) never retry — retrying garbage is how you build a very busy DLQ. Infrastructure failures (DB unreachable) are *not* dead-lettered: the offset is left uncommitted and Kafka redelivers.
- **Offsets commit per record, in order, never past a failure.** If record N hits an infrastructure error, records after N on that partition are left uncommitted too, so the tail is redelivered in order rather than skipped.
- **Ordering is per-account** (partition key = `from_account`), which serialises each account's *outgoing* transfers. It does not serialise credits from many senders — it doesn't need to: balance updates are atomic increments in the DB.
- **The poison mechanic is a demo device.** `reference == "POISON"` makes a simulated downstream dependency fail transiently, deterministically, so the retry→DLQ→replay arc is demoable on demand. Replay "fixing" it (`POISON` → `POISON-FIXED`) models an operator addressing the root cause before requeueing. Real systems would carry structured error taxonomies; the shape of the machinery is the same.
- **Known limits.** (1) Raw-published JSON events must carry UUID-shaped ids — a payload whose ids aren't UUIDs will fail the Postgres writes and be redelivered rather than dead-lettered (non-JSON payloads *are* handled: they're dead-lettered under a hash-derived id). A schema registry with validation at the producer edge is the production answer. (2) The dedup fast-path key TTL is 24h; the Postgres marker is forever, so correctness survives the TTL — only speed doesn't. (3) Processed-events and outbox tables grow unboundedly; production would add retention/archival jobs. (4) One consumer group, in-process retries — a stall on one partition's record delays that partition, by design (ordering).

## Testing

```bash
go test ./...                                   # unit suite (no infra needed)
go test -tags integration -timeout 15m ./integration/...   # + Docker: full E2E
```

The unit suite runs the whole pipeline against in-memory stores whose semantics mirror the SQL (unique constraints, atomic batches):

- **Exactly-once under concurrency** — one event delivered **100× from 100 goroutines** → 1 application, 99 duplicates, balance moved once, counters exact.
- **The invariant under randomised chaos** — 300 seeded transfers across 6 accounts, each delivered 1–5 times shuffled (≈900 deliveries, including order-dependent insufficient-funds cases), verified against a **shadow ledger** computed from the outcomes: every account balance matches, the total never moves.
- **Relay crash window** — a producer that publishes and *then* "crashes" before the outbox mark forces republication; the consumer drops the resulting duplicate.
- **Poison arc** — retries exhaust → DLQ with `attempts=3` → replay republishes the *same event_id* with the fix → applied once → its own redelivery dropped as a duplicate.
- **API idempotency** — replay returns the original transfer with the header; same-key/different-payload is a 409; validation matrix (missing key, bad amounts, self-transfer, unknown account).
- **Insufficient funds** — DLQ'd atomically, balance untouched, redelivery recognised as a duplicate.

The integration test (testcontainers-go: apache/kafka + postgres:17 + valkey) drives the *real* relay and consumer group through a 51-delivery duplicate storm and asserts balances, counters, and the invariant. It **skips cleanly** when no Docker daemon is available.

> **Verification note for this repo's history:** the project was authored in an environment *without* a Docker daemon, so the Compose stack and the integration test were verified by compilation (`go vet -tags integration ./...`), a clean runtime skip, and static config validation — the full-stack run itself was **not** executed there. The demo outputs above are labelled *expected* for that reason; on a Docker machine, `docker compose up` + the quickstart is the real proof, and the integration test will run for real. The unit suite's numbers, by contrast, are from actual runs: **18 tests, all passing** (including with `-race`).

## Future work

- OpenTelemetry tracing across ingest → relay → consume (one trace per event, dedup drops as span events).
- Schema registry (Avro/Protobuf) with producer-side validation — closes the non-UUID-id limit above.
- Kafka-side poison handling via a retry topic with delayed redelivery instead of in-process backoff.
- Outbox/processed-events retention jobs; partitioning `processed_events` by month.
- A tiny live web UI over `/stats` for demos (Grafana already covers the metrics story).
- Benchmarks: sustained throughput of the apply transaction, and dedup fast-path hit rates under different duplicate distributions.

## Tech

Go 1.27 · [franz-go](https://github.com/twmb/franz-go) (pure-Go Kafka client) · [pgx](https://github.com/jackc/pgx) · [go-redis](https://github.com/redis/go-redis) (Valkey) · [chi](https://github.com/go-chi/chi) · Prometheus + Grafana · testcontainers-go · Docker Compose (Kafka KRaft, Postgres 17, Valkey 8).
