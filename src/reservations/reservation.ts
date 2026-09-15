import { Guest, Money, ReservationEvent, ReservationStatus } from '../events/reservation-event.js'
import { compareOrderingKeys, OrderingKey } from '../events/ordering-key.js'

export type TransitionOutcome = 'applied' | 'stale' | 'rejected'

export interface TimelineEntry {
  eventId: string
  outcome: TransitionOutcome
  reason?: string
  recordedAt: string
}

export interface Reservation {
  reservationId: string
  status: ReservationStatus
  appliedKey: OrderingKey
  // /**
  //  * Mirrors the email ledger's state; applyTransition never sets this itself. Deciding
  //  * "was the email sent" here would couple reversible state to the irreversible effect,
  //  * which §1 explicitly says must not happen. The (future) orchestrator flips this after
  //  * the ledger — the actual source of truth — confirms a send.
  //  */
  // confirmationEmailSent: boolean
  guest: Guest
  propertyId: string
  dates: { checkIn: string; checkOut: string }
  totalAmount: Money
  timeline: TimelineEntry[]
}

export interface TransitionResult {
  reservation: Reservation
  outcome: TransitionOutcome
  reason?: string
}

/**
 * Pure state transition — no I/O, no locking. The per-reservation lock (§6) and the
 * arrival counter are the caller's responsibility; `arrivalSeq` is threaded in explicitly
 * rather than hidden inside the event, since it isn't part of the wire contract (§2) —
 * it's the consumer's own monotonic counter, used only as the ordering cascade's
 * last-resort tiebreaker (ordering-key.ts).
 */
export function applyTransition(
  current: Reservation | null,
  event: ReservationEvent,
  arrivalSeq: number,
): TransitionResult {
  const key: OrderingKey = { version: event.version, occurredAt: event.occurredAt, arrivalSeq }
  const recordedAt = new Date().toISOString()

  // No prior state: nothing for the ordering guard to compare against, and no prior
  // status for the cancelled-terminal rule to reject against either. This is what makes
  // "updated arriving before its created" (feed scenario 3) work: the first event seen
  // for a reservation is applied regardless of its `type`.
  if (current === null) {
    const reservation: Reservation = {
      reservationId: event.reservationId,
      status: event.payload.status,
      appliedKey: key,
      // confirmationEmailSent: false,
      guest: event.payload.guest,
      propertyId: event.payload.propertyId,
      dates: { checkIn: event.payload.checkIn, checkOut: event.payload.checkOut },
      totalAmount: event.payload.totalAmount,
      timeline: [{ eventId: event.eventId, outcome: 'applied', recordedAt }],
    }
    return { reservation, outcome: 'applied' }
  }

  // I2: an event whose ordering key is <= the one already applied does not mutate state.
  // The timeline still gets an entry — it's an audit log of everything evaluated, not
  // part of the "state" I2 protects.
  if (compareOrderingKeys(key, current.appliedKey) <= 0) {
    const reason = 'ordering key is not newer than the applied key'
    const reservation: Reservation = {
      ...current,
      timeline: [...current.timeline, { eventId: event.eventId, outcome: 'stale', reason, recordedAt }],
    }
    return { reservation, outcome: 'stale', reason }
  }

  // Past the ordering guard: this event is now the most recently *evaluated* one, whether
  // or not the state-machine rule below lets it change anything — appliedKey advances
  // either way, so a genuinely older event arriving after this one is still caught as stale.
  if (current.status === 'cancelled' && event.payload.status !== 'cancelled') {
    const reason = 'cancelled reservation is terminal'
    const reservation: Reservation = {
      ...current,
      appliedKey: key,
      timeline: [...current.timeline, { eventId: event.eventId, outcome: 'rejected', reason, recordedAt }],
    }
    return { reservation, outcome: 'rejected', reason }
  }

  // Full snapshot replaces the aggregate's copy wholesale (§2, §8 A2) — reservation-level
  // last-write-wins. confirmationEmailSent is carried forward untouched (see field comment).
  const reservation: Reservation = {
    ...current,
    status: event.payload.status,
    appliedKey: key,
    guest: event.payload.guest,
    propertyId: event.payload.propertyId,
    dates: { checkIn: event.payload.checkIn, checkOut: event.payload.checkOut },
    totalAmount: event.payload.totalAmount,
    timeline: [...current.timeline, { eventId: event.eventId, outcome: 'applied', recordedAt }],
  }
  return { reservation, outcome: 'applied' }
}

/**
 * The trigger is the transition, not the event type (§3): checking
 * `event.type === 'reservation.created'` breaks the moment an `updated` arrives before its
 * `created`. Exported so the caller has one correct place to ask this instead of
 * reimplementing the comparison — and getting it wrong — at each call site.
 */
export function crossedIntoConfirmed(previous: Reservation | null, next: Reservation): boolean {
  return previous?.status !== 'confirmed' && next.status === 'confirmed'
}
