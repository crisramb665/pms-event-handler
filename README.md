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

## Lint / format

```bash
pnpm lint
pnpm format
```
