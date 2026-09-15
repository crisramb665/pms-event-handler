# Reservation Event Sync

NestJS service that consumes reservation lifecycle events off RabbitMQ and converges them
to a local, idempotent view. See [spec.md](./spec.md) for the contract this is built
against, and [CLAUDE.md](./CLAUDE.md) for the working agreement/constraints.

## Setup

```bash
pnpm install
docker compose up -d   # RabbitMQ, management UI at localhost:15672 (guest/guest)
```

## Run

```bash
pnpm start:dev
```

## Test

```bash
pnpm test        # unit
pnpm test:e2e     # e2e
```

## Simulated feed

Runs the eight scenarios from [spec.md §9](./spec.md) against the real pipeline and prints
expected vs. actual state, email count and dead-letters per scenario. Exit code 1 on any
failure.

```bash
pnpm simular                                    # against RabbitMQ (docker compose up -d first)
MESSAGE_BUS_DRIVER=memory pnpm simular          # no Docker, same semantics
RETRY_DELAYS_MS=200,500 pnpm simular            # fast retries for a live demo
```

Retry delays come from `RETRY_DELAYS_MS` (comma-separated ms, one entry per retry tier;
default `5000,30000`, i.e. the 5s / 30s tiers from §6). The simulator never changes them —
it only waits longer. Scenario 8 (email sender down twice) has to sit through both tiers, so
with the defaults it takes ~35s; the low values above exist purely so a demo doesn't stall.

RabbitMQ side note: retry queues are named after their TTL (`reservations.retry.5s`), because
the broker refuses to redeclare an existing queue with a different `x-message-ttl`. Changing
`RETRY_DELAYS_MS` therefore creates new queues rather than failing at boot; the old ones stay
around until dropped by hand.

## Lint / format

```bash
pnpm lint
pnpm format
```
