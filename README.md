# Reservation Event Sync

Keeps a local view of reservations in sync with an unreliable stream of PMS-like events,
and sends a confirmation email **at most once per reservation** under duplicates,
reordering, and sender failures.

Design was written first, in [SPEC.md](./SPEC.md), and the code was built against it. The
spec is the contract; this README is what happened when it met the time-box.

---

## The problem, restated

The interesting part of this brief is not "sync some records". It is:

> When does a state change justify an **irreversible side effect**, and how do you keep
> that effect from firing twice when the transport won't cooperate?

Two consequences drove every decision below.

**State is reversible and convergent; effects are not.** A wrong status is corrected by
the next event. A duplicate email cannot be unsent. These are handled by separate
mechanisms that are deliberately not coupled.

**"Exactly once" is not a delivery guarantee.** It does not exist over an unreliable
network. What exists is at-least-once delivery plus an idempotent consumer, which yields
exactly-once _effect_. RabbitMQ is not the solution to this problem — a lost ack or a
dropped connection makes it one of the _causes_. Everything here assumes redelivery is
normal, not exceptional.

---

## Running it

```bash
docker compose up -d rabbitmq      # RabbitMQ 4 + management UI on :15672 (guest/guest)
pnpm install
pnpm test                          # unit tests — no Docker needed, runs on the in-memory bus
pnpm simulate                      # end-to-end: publishes the feed through real RabbitMQ
```

`pnpm simulate` prints each scenario as it runs and finishes with a pass/fail table. It
exits non-zero if any scenario fails.

> **Only one consumer process at a time.** Running `pnpm start:dev` and `pnpm simulate`
> together puts two consumers on `reservations.events`, and RabbitMQ round-robins between
> them — the simulator then waits for events its own process never received and times out.
> This is the single-consumer FIFO constraint from §6 showing its teeth; it is exactly what
> partitioning by `reservation_id` would solve. (Found the hard way, on my own machine.)

Retry queue names are derived from `RETRY_DELAYS_MS`, so changing that value creates a new
pair of queues rather than mutating the existing ones — RabbitMQ rejects TTL changes on an
already-declared queue. Messages left in the old queues are stranded. Reset with
`docker compose down -v && docker compose up -d rabbitmq` when switching configurations
(`restart` is not enough; the image declares a volume that survives it).

The last scenario exercises the retry chain, so a default run takes ~40s. For a faster
loop:

```bash
RETRY_DELAYS_MS=1000,2000 pnpm simulate
```

Queues accumulate across runs (they're durable). To start clean:
`docker compose restart rabbitmq`.

### Configuration

| Variable             | Default                                             | Notes                                                |
| -------------------- | --------------------------------------------------- | ---------------------------------------------------- |
| `MESSAGE_BUS_DRIVER` | `rabbitmq`                                          | `memory` swaps in the in-process adapter.            |
| `RABBITMQ_URL`       | `amqp://guest:guest@localhost:5672/%2Freservations` | vhost is URL-encoded.                                |
| `RETRY_DELAYS_MS`    | `5000,30000`                                        | One entry per retry tier. `maxAttempts = tiers + 1`. |

---

### Inspection endpoints

| Endpoint | Returns |
|---|---|
| `GET /reservations` | Current local view of all reservations |
| `GET /reservations/:id` | One reservation plus its email ledger state |
| `GET /reservations/:id/timeline` | Every event evaluated for that reservation — applied, stale or rejected, and why |
| `GET /dlq` | Dead-lettered messages with failure reasons |
| `GET /metrics` | Consumer and bus counters, plus reservations stuck in `reserved` |

These return domain objects directly. No DTOs, no versioning — this is an inspection
surface for the exercise, not a public API.

State is in-memory and per-process, so these reflect only what *that* process consumed.
`pnpm simulate` runs its own short-lived process and exits, so a separately running server
shows zeros. A database (see "Scaling to production") removes the split.

## How it works

```
publish ──► RabbitMQ ──► consumer pipeline
                            │
                            ├─ 1. parse envelope ──── invalid? → DLQ, no retry
                            ├─ 2. dedupe by event_id (fast path)
                            ├─ 3. lock on reservation_id
                            ├─ 4. dedupe again, inside the lock   ← the one that counts
                            ├─ 5. load reservation
                            ├─ 6. ordering guard: stale? → record, ack
                            ├─ 7. state rule: cancelled is terminal → record, ack
                            ├─ 8. apply transition
                            ├─ 9. crossed into CONFIRMED? → email ledger → send
                            ├─ 10. save reservation
                            └─ 11. mark event_id processed   ← last, never first
```

### Invariants

The deliverable is really this list. Each maps to a scenario in the simulated feed.

| #   | Invariant                                                               | Mechanism                               |
| --- | ----------------------------------------------------------------------- | --------------------------------------- |
| I1  | Processing the same `event_id` twice is a no-op                         | dedupe store, checked under the lock    |
| I2  | An event not newer than the applied one does not mutate state           | ordering-key comparator                 |
| I3  | At most one confirmation email per reservation                          | effect ledger keyed by `reservation_id` |
| I4  | A persistently failing event reaches the DLQ without blocking the queue | retry tiers + dead-lettering            |
| I5  | Final **state** is independent of arrival order                         | see the caveat below                    |

---

## Decisions worth defending

### The email hangs off the transition, not the event type

The trigger is `previous.status !== CONFIRMED && next.status === CONFIRMED`, not
`event.type === 'reservation.created'`. The obvious version breaks the moment an `updated`
arrives before its `created` — which is scenario 3 in the feed, and which the brief
explicitly says happens in production.

### Deduplication and ordering are two mechanisms, not one

Dedupe by `event_id` defends against _the same message_ arriving twice. It does nothing
about _two different messages_ arriving in the wrong order. Only having one of the two
leaves half the brief unsolved. They are separate steps in the pipeline and separate
scenarios in the feed.

### Ordering is a cascade, not a field

`version` if present, else `occurred_at`, else arrival sequence. Real PMS webhooks
frequently expose only a timestamp, and two events can share a millisecond. Isolating this
in `ordering-key.ts` means that if the source turns out to be weaker than assumed, one
file changes. This was one of the open questions sent during the window (A1 in SPEC.md
§8).

### The email is sent _before_ the reservation is saved

This ordering is load-bearing and not obvious. If the save happened first and the send
then failed, the retry would find `appliedKey` already advanced, mark the event `stale`,
and never re-evaluate the crossing — **the email would never be sent at all**. Swapping
these two lines silently breaks the central requirement of the brief.

### The effect ledger reserves before it sends

`reserve → send → markSent`, never the reverse. A crash between reserve and send leaves a
stuck `reserved` entry and no email. A crash between send and record would produce a
_duplicate_ email on retry. Under this ordering, a crash can lose an email; it can never
duplicate one.

That trade is deliberate — the brief names duplicate guest messages as the real-world
pain, and a missing email is recoverable while a duplicate is not. Stuck entries are
observable via `pendingReservations()` rather than silent. Transient send failures are
different from crashes: there the ledger entry is explicitly released before the error
propagates, so the retry can send.

### The event_id is marked processed last

If it were marked first, any failure after that point would make the redelivery look like
a duplicate and get dropped — silent message loss. Marking last means a crash mid-pipeline
causes reprocessing, which is exactly what the idempotency exists for.

### Dedupe is checked twice

Once outside the lock as a fast path, once inside as the actual guarantee. Check-then-act
outside a critical section is not a guarantee: two concurrent deliveries of the same
`event_id` can both pass the outer check before either marks it processed. Today, with
`prefetch=1` and a single consumer, only the outer check ever fires. The inner one exists
for the moment concurrency goes up.

### RabbitMQ, declared explicitly, with Nest's transport deliberately unused

Nest ships an RMQ transport. It abstracts away exactly what this exercise is about:
topology declaration, manual ack, prefetch, the retry chain. `amqp-connection-manager` is
used directly inside a Nest provider instead, with all four queues declared in code.

Two details that are easy to get wrong:

- **TTL is set per queue, never per message.** A queue with heterogeneous per-message TTLs
  only expires from the head, so a 30s message blocks the 5s messages behind it.
- **Retry queues dead-letter back to the main exchange**, not to the queue, so the binding
  stays intact.

### Retry is publish-then-ack, and that window is acceptable

On transient failure the message is republished to the next retry tier and the original is
acked. A crash between those two operations means the message is both requeued _and_
redelivered. That is tolerable precisely because the consumer is idempotent — the same
property the whole system rests on. The alternative, `nack` with requeue, puts the message
back at the head with no backoff and busy-loops.

The attempt counter rides in the `x-attempt` header rather than in process memory, so a
consumer restart doesn't reset it and a poison message can't retry forever.

---

## Known gaps

Listed because a green suite that passes for unverified reasons is worse than a stated
gap.

### Two feed scenarios pass for the wrong reason

I ran manual mutation testing on the guarantees: disable the ordering guard, re-run the
feed, and check that the scenarios claiming to test it actually fail.

Only scenario 3 failed. Scenarios 4 and 5 stayed green — the terminal-state rule and the
email ledger respectively were catching what the ordering guard was supposed to catch. So
of the three scenarios that claim to verify I2, one actually isolates it.

The fix is to reshape those event sequences so each failure has exactly one defence in
front of it (in scenario 4, the late event must arrive while the reservation is _not_ yet
cancelled; in scenario 5, the last-published event must carry a status that differs from
the expected final one). I ran out of time-box before doing it.

### I5 does not extend to effects, by design

The email fires on an **observed** transition into CONFIRMED. If the confirming event
arrives after a later event has already advanced the ordering key, the guard drops it as
stale and the crossing is never observed — so one permutation can legitimately produce
zero emails where another produces one.

State still converges in every permutation. Extending the guarantee to effects would
require either deriving effects from final state (needs a grace window, i.e. latency) or
replaying full event history per reservation (event sourcing, out of scope). The invariant
actually held is: **at most one confirmation email per reservation, never two.**

The scenario that demonstrates this cleanly — `created(pending,v1)`, `cancelled(v3)`,
`updated(confirmed,v2)` → state `cancelled`, **0 emails** — was specified but not
implemented before the time-box closed.

### Counters over-count retried attempts

An event that fails at the email step and is retried increments `applied` once per
attempt. The behaviour is correct; the label isn't. They should be read as
attempts-per-outcome, not events-per-outcome.

### Scenario 5 isn't actually concurrent

`Promise.all` parallelises _publishing_. With `prefetch=1` and a single consumer, delivery
is still sequential — so it exercises reordering, not concurrency. Real concurrency needs
`CONSUMER_CONCURRENCY > 1`, which is where the per-reservation lock earns its place.


## Explicitly not built

These were excluded as decisions, not omissions. None of them touches an invariant.

| Excluded                           | Why                                                                                                      | Where it would plug in                                                         |
| ---------------------------------- | -------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| Real database                      | Brief says optional; the hard part is ordering and effect semantics, not storage                         | Repository interface (see below)                                               |
| UI                                 | Brief says the sync logic matters more than any screen                                                   | —                                                                              |
| Field-level merge                  | Payloads are full snapshots, so reservation-level LWW is correct. With deltas it isn't.                  | `applyTransition` is one pure function                                         |
| Cross-instance coordination        | The in-process lock covers one process                                                                   | Partition by `reservation_id`, or optimistic concurrency on the version column |
| Event sourcing / replay            | The processed-event log gives cheap auditability; replay is an architecture, not a feature               | —                                                                              |
| Auth, multi-tenancy, rate limiting | Orthogonal to every invariant                                                                            | —                                                                              |
| Dedupe store eviction              | Unbounded growth is acknowledged, not solved                                                             | TTL'd table or Redis set with expiry                                           |
| Reconciliation against the source  | The right production answer for unresolvable conflicts, but needs a PMS read API that doesn't exist here | —                                                                              |

---

## Scaling to production

**Persistence.** Replace the in-memory repositories behind their existing interfaces:

- `processed_events (event_id PRIMARY KEY, processed_at)` — the unique constraint _is_ the
  dedupe, and it's atomic in a way the in-memory `Set` is only by virtue of single-threaded
  Node.
- Reservations with a `version` column, updated via
  `UPDATE ... WHERE reservation_id = $1 AND applied_version < $2`. Zero rows affected means
  a concurrent writer won — no lock needed. This replaces `KeyedMutex` rather than
  complementing it.
- The email ledger becomes a unique constraint on `reservation_id` plus a state column with
  a `reserved_at` timestamp, so a sweeper can reclaim entries stuck in `reserved`.

**Transactional outbox for the email.** The current design keeps the send inside the
message handler, which is why it can lose an email on a crash. In production, the state
change and an outbox row commit in one transaction, and a separate relay sends and marks
the row. That closes the gap without opening a duplicate window.

**Concurrency.** Partition by `reservation_id` — consistent-hash exchange or one queue per
shard — so ordering holds per reservation while throughput scales across them. The
single-consumer constraint here is the trade for keeping FIFO without extra infrastructure.

**Observability.** Structured logs already carry `event_id` and `reservation_id` on every
branch. The next steps are exporting the counters to Prometheus, alerting on DLQ depth and
on ledger entries stuck in `reserved`, and adding trace correlation from webhook receipt
through to send.

**Reconciliation.** Webhooks get dropped. A periodic pull of reservations changed in the
last N hours, diffed against local state, is the only thing that catches events that never
arrived at all. No amount of consumer-side correctness substitutes for it.

---

## What I'd do differently with more time

In priority order:

1. **Fix the two scenarios that pass for the wrong reason**, and add the zero-email
   scenario. Verified coverage beats more coverage.
2. **Property-based test over feed permutations** asserting state convergence and
   `emails <= 1`. Highest confidence per line of test code in this problem.
3. **A DLQ reprocessing endpoint.** Dead-lettering without a way back is half a mechanism.
4. **Run the bus contract tests against both adapters.** They currently only run against
   the in-memory one, which means the port is asserted rather than proven.

---

## AI usage

Built with Claude Code against `SPEC.md` and a `CLAUDE.md` working agreement, which is why
the spec was written before any code — it was the context the generation was steered by,
not documentation written afterward.

Generation was used aggressively for domain code and conservatively for RabbitMQ topology,
where models routinely produce stale queue arguments and outdated retry patterns. Every
queue argument was verified against the management UI before being accepted. The design
decisions in this README are mine; the typing was not.

---

