# Reservation Event Sync — Specification

Written before implementation. This is the contract the code is built against and the
reference used to review it afterwards.

**Status:** draft, pre-implementation

---

## 1. Problem

An external PMS-like source emits reservation lifecycle events over a webhook. The
transport is unreliable in three distinct ways:

1. **Exact duplicates** — the same event delivered more than once (source retry, broker
   redelivery, lost ack).
2. **Out-of-order delivery** — a newer event is processed before an older one.
3. **Late arrivals** — an old event shows up after the reservation has already moved on.

The local view must converge to the correct state regardless, and a confirmation email
must reach the guest **once and only once per reservation**.

### The core distinction

**State is reversible and convergent. Effects are irreversible.**

A wrong local state can be corrected by the next event. A duplicate email cannot be
unsent. These are handled by different mechanisms and must not be coupled:

- State correctness → ordering guard + deduplication.
- Effect correctness → a durable, idempotent effect ledger keyed by the effect's own
  identity, not by the event that triggered it.

### On "exactly once"

Exactly-once *delivery* does not exist over an unreliable network. What is achievable is
at-least-once delivery with an idempotent consumer, which yields exactly-once *effect*
("effectively once"). Everything below is designed on that assumption. RabbitMQ is not a
solution to this problem — it is one of the sources of it, since a lost ack or a
connection drop causes redelivery by design.

---

## 2. Event contract

Defined by us (the brief allows it). Envelope and payload are separated: the envelope is
transport metadata, the payload is a full reservation snapshot.

```jsonc
{
  // --- envelope ---
  "event_id":       "evt_01H8X...",        // unique per emission; dedup key
  "reservation_id": "res_4821",            // aggregate key; partition key
  "type":           "reservation.updated", // created | updated | cancelled
  "version":        7,                     // monotonic per reservation; nullable
  "occurred_at":    "2026-09-14T10:32:11.482Z", // source-side event time

  // --- payload: full snapshot, not a delta ---
  "payload": {
    "reservation_id": "res_4821",
    "status":         "confirmed",         // pending | confirmed | cancelled
    "guest": { "id": "gst_99", "email": "guest@example.com", "name": "Ada L." },
    "property_id":    "prop_12",
    "check_in":       "2026-10-02",
    "check_out":      "2026-10-06",
    "total_amount":   { "value": 184000, "currency": "USD" }
  }
}
```

**Full snapshot, not delta** — assumption, see §8. With deltas, last-write-wins at the
reservation level silently loses data and per-field versioning becomes necessary.

### Ordering key

Ordering does not depend on a single field. It is a comparator, so the system degrades
gracefully when the source is weaker than assumed:

1. `version` if present on both events — strict comparison.
2. Otherwise `occurred_at` — weaker: two events can share a millisecond.
3. Otherwise arrival sequence — last resort, only breaks ties.

Real PMS webhooks frequently expose only a timestamp. The comparator is isolated in one
module (`OrderingKey.compare`) so that swapping the source's guarantees changes one file.

---

## 3. State machine

```
                  ┌──────────┐
   created ──────►│ PENDING  │
                  └────┬─────┘
                       │ updated(status=confirmed)
                       ▼
                  ┌───────────┐   ──► emit ConfirmationEmail (once per reservation)
                  │ CONFIRMED │
                  └────┬──────┘
                       │ cancelled
                       ▼
                  ┌───────────┐
                  │ CANCELLED │  (terminal)
                  └───────────┘
```

Rules:

- **`CANCELLED` is terminal.** A later event that would reconfirm the reservation is
  rejected on the state rule, recorded in the timeline, and acked. The ordering guard
  alone does not cover this: a *newer* event can still be an invalid transition.
- **The email hangs off the transition, not off the event type.** The trigger is
  `previous.status !== CONFIRMED && next.status === CONFIRMED`. Firing on
  `type === "reservation.created" && status === "confirmed"` breaks the moment an
  `updated` arrives before its `created`.
- **One confirmation email per `reservation_id`, for the lifetime of the reservation** —
  not per transition. A reservation that is confirmed, cancelled and confirmed again does
  not produce a second email. See §8.

---

## 4. Invariants

These are the deliverable. Each maps 1:1 to a test.

| # | Invariant |
|---|---|
| **I1** | Processing the same `event_id` N times leaves the same state as processing it once. |
| **I2** | An event whose ordering key is `<=` the one already applied does not mutate state. |
| **I3** | At most one confirmation email exists per reservation, under duplicates, reordering, and sender retries. |
| **I4** | An event that fails persistently lands in the DLQ without blocking the queue or corrupting state. |
| **I5** | Final state is independent of arrival order: any permutation of the same event set converges to the same result. |

I5 is the strongest of the five and the cheapest to test — permute the feed and assert
convergence.

---

## 5. Processing pipeline

Single path, one aggregate at a time:

```
consume
  └─ 1. parse + validate envelope ────── invalid? → DLQ immediately, no retry
  └─ 2. dedupe: event_id already processed? ──── yes → ack, drop (I1)
  └─ 3. acquire per-reservation lock
  └─ 4. load reservation (or empty aggregate)
  └─ 5. ordering guard: key <= applied? ──────── yes → record as "stale", ack (I2)
  └─ 6. apply transition ────────────────────── invalid? → record "rejected", ack
  └─ 7. detect crossing into CONFIRMED
  └─ 8. if crossed → reserve effect in ledger (idempotency key = reservation_id)
  └─ 9. persist: {state, applied key, event_id, timeline entry} atomically
  └─ 10. ack
```

Steps 2–9 are **in-memory and synchronous under the lock**, so "atomically" is trivially
satisfied. The production mapping of each step is in the README.

### Failure handling

- **Deterministic failures** (schema violation, unknown event type) → straight to DLQ.
  Retrying a poison message is a busy loop.
- **Transient failures** (simulated email sender down) → retry with backoff, capped at
  3 attempts, then DLQ.
- A failure never results in a bare ack. Silently swallowing an exception and acking is
  invisible message loss, which is the worst outcome in this class of system.

### The email is the only irreversible step

It is emitted through a ledger: reserve the intent keyed by `reservation_id`, then send,
then mark as sent. If the process dies between reserve and send, the reservation has a
reserved-but-unsent effect, which is recoverable. The reverse order — send then record —
is not. Under this design a crash can produce a *missing* email, never a duplicate one;
that trade is deliberate and stated because the brief's cost function is asymmetric.

---

## 6. RabbitMQ topology

Real broker, declared explicitly in code at boot.

```
  publisher ──► exchange: reservations (direct, durable)
                    │  rk: reservation.event
                    ▼
                queue: reservations.events
                    │  x-dead-letter-exchange: reservations.dlx
                    │  prefetch = 1, manual ack, single consumer
                    │
      on transient failure: republish with x-attempt++ ──┐
                    │                                     │
                    ▼                                     ▼
         queue: retry.5s        queue: retry.30s   (x-message-ttl per queue,
              │                      │              DLX back to `reservations`)
              └──────────┬───────────┘
                         ▼
                 back to reservations.events
                         │
             attempts exhausted / poison
                         ▼
                 queue: reservations.dlq
```

Decisions worth defending:

- **TTL is set per queue, never per message.** A queue with heterogeneous per-message
  TTLs only expires from the head, so a 30s message blocks the 5s messages behind it.
  Classic bug.
- **RabbitMQ has no native backoff.** The pattern is TTL + dead-lettering. Two tiers
  (5s / 30s) is enough to demonstrate it; more tiers add no new idea.
- **Ordering comes from `prefetch=1` + a single consumer.** RabbitMQ guarantees FIFO per
  queue only with one consumer. `CONSUMER_CONCURRENCY` is explicit config, defaulting to
  1. Scaling requires partitioning by `reservation_id` (consistent-hash exchange or one
  queue per shard) — documented, not built.
- **Nest's built-in RMQ transport is deliberately not used.** It abstracts away exactly
  what this exercise is meant to demonstrate: topology declaration, manual ack, prefetch,
  the retry chain. `amqp-connection-manager` is used directly inside a Nest provider.
- **Retry republish is publish-then-ack**, which leaves a crash window where a message is
  both requeued and redelivered. That is acceptable precisely because the consumer is
  idempotent — the same property that makes the whole system work.
- **The per-reservation lock is kept even though a single consumer makes it redundant.**
  It is the safety net for raising concurrency, which is a likely live-session change.

The broker sits behind a `MessageBus` port. An in-memory adapter implements the same
semantics (ack / nack / backoff / DLQ) and is used by the test suite, so tests run without
Docker. The port is a testability decision, not architectural decoration.

---

## 7. Non-goals

Excluded on purpose. None of these touch an invariant.

| Excluded | Reasoning | Extension point |
|---|---|---|
| Real database | Brief states it is optional. Complexity lives in ordering and effect semantics, not storage. | Repository interface; Postgres mapping documented in README. |
| UI | Brief states the sync logic matters more than any screen. | Read endpoints serve as the inspection surface. |
| Field-level merge | Requires delta payloads; full snapshots make reservation-level LWW correct. | `applyTransition` is one pure function. |
| Auth / multi-tenancy / rate limiting | Orthogonal to every invariant. | — |
| Cross-instance coordination | In-memory lock covers one process. Multi-instance needs partitioning or optimistic concurrency on the version column. | Documented, not built. |
| Full event sourcing / replay | The processed-event log gives cheap auditability. State reconstruction by replay is an architecture, not a 4-hour feature. | — |
| Schema registry / event versioning | One producer, one contract, defined here. | — |
| Reconciliation against the source | The correct production answer for unresolvable conflicts, but requires a PMS read API that does not exist in a synthetic environment. | README, "scaling to production". |
| Unbounded dedupe store growth | Acknowledged, not solved. In production this is a TTL'd table or a Redis set with expiry. | Noted in README. |

---

## 8. Open assumptions

Sent to the team as questions. These defaults hold unless they answer otherwise; whatever
they answer gets recorded here.

| # | Question | Default assumed |
|---|---|---|
| A1 | Does the source expose a monotonic version per reservation, or only `updatedAt`? | Comparator: version → `occurred_at` → arrival order. |
| A2 | Full snapshots or deltas? | Full snapshots; reservation-level LWW. |
| A3 | Is `cancelled` terminal, or can a reservation be reconfirmed? | Terminal. |
| A4 | If a reservation could be reconfirmed, is a second email sent? | No — one email per reservation, lifetime. |
| A5 | Same `event_id` arriving with a *different* payload — corruption or legitimate correction? | Corruption. The id is the identity; the second delivery is dropped and logged as a conflict. |

A5 is worth noting: it is the one case where dedup-by-id is a judgement call rather than
an obvious win.

---

## 9. Simulated event feed

The feed is ours to design, so the scenarios it contains are a deliberate statement about
which failures were anticipated:

1. Exact duplicate, delivered back to back.
2. Deferred duplicate — same `event_id`, arriving much later.
3. `updated` arriving before its `created`.
4. `cancelled` followed by a stale `updated` that would reconfirm it (exercises §3 rule,
   not the ordering guard).
5. Concurrent burst on a single `reservation_id`.
6. Same `event_id`, mutated payload (A5).
7. Poison message — malformed envelope, must reach the DLQ without stalling the queue.
8. Email sender failing twice then succeeding — verifies I3 survives sender retries.

---

## 10. Observability

Minimum viable, but present from the start rather than bolted on:

- Structured logs, every line carrying `event_id` and `reservation_id`.
- Counters: received, deduplicated, stale-dropped, rejected-by-rule, applied, emails sent,
  retries, DLQ.
- `GET /reservations` — current local view.
- `GET /reservations/:id/timeline` — per event: applied, dropped, or rejected, and why.
- `GET /dlq` — dead-lettered messages with their failure reason.
- `GET /metrics` — the counters above.

The timeline endpoint is the main demonstration surface: it makes the ordering and dedup
decisions visible instead of asserted.