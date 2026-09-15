import type { ReservationStatus } from '../events/reservation-event.js'

/**
 * The simulated feed from SPEC.md §9. Each scenario is a statement about a failure mode
 * that was anticipated; `expected` is what the local view must converge to regardless.
 * Every scenario owns a distinct reservation_id so they cannot influence each other.
 */
export interface Scenario {
  name: string
  description: string
  reservationId: string
  /** Raw wire payloads (snake_case, as the PMS would send them). Deliberately untyped:
   *  one scenario is a malformed envelope, and typing the feed would make that impossible. */
  events: unknown[]
  /** Publish all events with Promise.all instead of one awaited publish at a time. */
  concurrent?: boolean
  /** Simulator-only: makes the next N confirmation sends fail (SimulatedEmailSender.failNextN). */
  failEmailSends?: number
  expected: {
    status: ReservationStatus | null
    emails: number
    /** Defaults to 0 — any unexpected dead-letter fails the scenario. */
    deadLetters?: number
  }
}

interface WireOptions {
  type?: 'reservation.created' | 'reservation.updated' | 'reservation.cancelled' | string
  version: number
  status: ReservationStatus
  eventId?: string
  amount?: number
}

const BASE_TIME = Date.parse('2026-09-14T10:00:00.000Z')

/** occurred_at advances one minute per version so the timestamp fallback of the ordering
 *  cascade agrees with `version` — the scenarios below are about ordering, not clock skew. */
function wire(reservationId: string, opts: WireOptions): unknown {
  return {
    event_id: opts.eventId ?? `evt_${reservationId}_v${opts.version}`,
    reservation_id: reservationId,
    type: opts.type ?? 'reservation.updated',
    version: opts.version,
    occurred_at: new Date(BASE_TIME + opts.version * 60_000).toISOString(),
    payload: {
      reservation_id: reservationId,
      status: opts.status,
      guest: { id: 'gst_sim', email: 'guest@example.com', name: 'Ada L.' },
      property_id: 'prop_12',
      check_in: '2026-10-02',
      check_out: '2026-10-06',
      total_amount: { value: opts.amount ?? 184_000, currency: 'USD' },
    },
  }
}

const created = (id: string) => wire(id, { type: 'reservation.created', version: 1, status: 'pending' })
const confirmed = (id: string, version: number, extra: Partial<WireOptions> = {}) =>
  wire(id, { version, status: 'confirmed', ...extra })
const cancelled = (id: string, version: number) =>
  wire(id, { type: 'reservation.cancelled', version, status: 'cancelled' })

export const SCENARIOS: Scenario[] = [
  {
    name: '1. exact duplicate',
    description: 'The confirming event is delivered twice back to back (same event_id). One email.',
    reservationId: 'res_sim_1',
    events: [created('res_sim_1'), confirmed('res_sim_1', 2), confirmed('res_sim_1', 2)],
    expected: { status: 'confirmed', emails: 1 },
  },
  {
    name: '2. deferred duplicate',
    description: 'Same event_id as the earlier confirm, redelivered after the reservation was cancelled. Dropped by dedup (I1), never reaches the state machine.',
    reservationId: 'res_sim_2',
    events: [
      created('res_sim_2'),
      confirmed('res_sim_2', 2),
      cancelled('res_sim_2', 3),
      confirmed('res_sim_2', 2), // same event_id as the earlier confirm
    ],
    expected: { status: 'cancelled', emails: 1 },
  },
  {
    name: '3. updated before created',
    description: 'updated(confirmed) v2 arrives first; created(pending) v1 arrives after and is stale (I2). The email fires on the transition, not on event type.',
    reservationId: 'res_sim_3',
    events: [confirmed('res_sim_3', 2), created('res_sim_3')],
    expected: { status: 'confirmed', emails: 1 },
  },
  {
    name: '4. reconfirm after cancel',
    description:
      'After cancellation (v3), a reconfirm with a lower key (v2) is stale by the ordering guard, and a reconfirm with a newer key (v4) is rejected by the terminal-state rule (§3). Both paths, one scenario.',
    reservationId: 'res_sim_4',
    events: [
      created('res_sim_4'),
      confirmed('res_sim_4', 2),
      cancelled('res_sim_4', 3),
      confirmed('res_sim_4', 2, { eventId: 'evt_res_sim_4_late_v2' }),
      confirmed('res_sim_4', 4),
    ],
    expected: { status: 'cancelled', emails: 1 },
  },
  {
    name: '5. concurrent burst',
    description:
      'Five events for one reservation published with Promise.all, out of version order. Converges to v5 (I5) with exactly one email whichever event happens to cross into CONFIRMED first.',
    reservationId: 'res_sim_5',
    concurrent: true,
    events: [
      confirmed('res_sim_5', 3, { amount: 190_000 }),
      created('res_sim_5'),
      confirmed('res_sim_5', 5, { amount: 210_000 }),
      confirmed('res_sim_5', 2, { amount: 184_000 }),
      confirmed('res_sim_5', 4, { amount: 200_000 }),
    ],
    expected: { status: 'confirmed', emails: 1 },
  },
  {
    name: '6. same event_id, mutated payload',
    description:
      '§8 A5: the confirm event_id is redelivered carrying a cancelled payload. The id is the identity, so the second delivery is dropped as a duplicate. (Logged as deduplicated — payload-hash conflict detection is not built.)',
    reservationId: 'res_sim_6',
    events: [
      created('res_sim_6'),
      confirmed('res_sim_6', 2),
      wire('res_sim_6', { eventId: 'evt_res_sim_6_v2', type: 'reservation.cancelled', version: 3, status: 'cancelled' }),
    ],
    expected: { status: 'confirmed', emails: 1 },
  },
  {
    name: '7. poison message',
    description:
      'Unknown event type is a deterministic failure: NonRetryableError, straight to the DLQ with no retry. The valid event behind it must still be processed — the queue does not stall.',
    reservationId: 'res_sim_7',
    events: [wire('res_sim_7', { type: 'reservation.exploded', version: 1, status: 'pending' }), created('res_sim_7')],
    expected: { status: 'pending', emails: 0, deadLetters: 1 },
  },
  {
    name: '8. email sender down twice',
    description:
      'The sender fails on the first two attempts. The confirm event goes through both retry tiers (RETRY_DELAYS_MS) and succeeds on the third; I3 holds — one email, not three.',
    reservationId: 'res_sim_8',
    failEmailSends: 2,
    events: [created('res_sim_8'), confirmed('res_sim_8', 2)],
    expected: { status: 'confirmed', emails: 1 },
  },

]
