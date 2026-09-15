/**
 * Thrown when a failure is deterministic (malformed envelope, unknown event type, a
 * business rule that will never pass on retry). Retrying a poison message is a busy
 * loop, so anything thrown as this type skips the retry chain and is dead-lettered
 * immediately. Lives here, not in the messaging layer, because the classification
 * ("this will never succeed") is a domain judgement — the bus only reacts to it.
 */
export class NonRetryableError extends Error {
  constructor(
    message: string,
    readonly cause?: unknown,
  ) {
    super(message)
    this.name = 'NonRetryableError'
  }
}

export type ReservationEventType = 'reservation.created' | 'reservation.updated' | 'reservation.cancelled'

export type ReservationStatus = 'pending' | 'confirmed' | 'cancelled'

export interface Money {
  value: number
  currency: string
}

export interface Guest {
  id: string
  email: string
  name: string
}

/** Full reservation snapshot, not a delta — §2, §8 A2. Every applied event replaces the
 *  aggregate's copy of these fields wholesale (reservation-level last-write-wins). */
export interface ReservationSnapshot {
  reservationId: string
  status: ReservationStatus
  guest: Guest
  propertyId: string
  checkIn: string
  checkOut: string
  totalAmount: Money
}

/** Envelope and payload are kept separate on purpose: the envelope is transport
 *  metadata (identity, ordering, routing), the payload is the domain snapshot. */
export interface ReservationEvent {
  eventId: string
  reservationId: string
  type: ReservationEventType
  /** Monotonic per reservation when the source provides it; null otherwise (§8 A1). */
  version: number | null
  occurredAt: string
  payload: ReservationSnapshot
}

const EVENT_TYPES: ReadonlySet<string> = new Set([
  'reservation.created',
  'reservation.updated',
  'reservation.cancelled',
])

const STATUSES: ReadonlySet<string> = new Set(['pending', 'confirmed', 'cancelled'])

const DATE_ONLY = /^\d{4}-\d{2}-\d{2}$/ // YYYY-MM-DD, matching the example payload in §2

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0
}

function fail(reason: string): never {
  throw new NonRetryableError(`invalid reservation event: ${reason}`)
}

function parseSnapshot(raw: unknown, envelopeReservationId: string): ReservationSnapshot {
  if (!isRecord(raw)) fail('payload must be an object')

  if (raw.reservation_id !== envelopeReservationId) {
    // Same identity space, two different values in one message — not a delta conflict
    // (§8 A5 is about a repeated event_id), just an internally inconsistent envelope.
    fail('payload.reservation_id does not match the envelope reservation_id')
  }

  if (!isNonEmptyString(raw.status) || !STATUSES.has(raw.status)) {
    fail(`payload.status must be one of pending|confirmed|cancelled, got ${String(raw.status)}`)
  }

  const guestRaw = raw.guest
  if (
    !isRecord(guestRaw) ||
    !isNonEmptyString(guestRaw.id) ||
    !isNonEmptyString(guestRaw.email) ||
    !isNonEmptyString(guestRaw.name)
  ) {
    fail('payload.guest must have non-empty id, email and name')
  }
  if (!guestRaw.email.includes('@')) fail('payload.guest.email is not a valid email address')

  if (!isNonEmptyString(raw.property_id)) fail('payload.property_id must be a non-empty string')

  if (!isNonEmptyString(raw.check_in) || !DATE_ONLY.test(raw.check_in))
    fail('payload.check_in must be a YYYY-MM-DD date')

  if (!isNonEmptyString(raw.check_out) || !DATE_ONLY.test(raw.check_out))
    fail('payload.check_out must be a YYYY-MM-DD date')

  const amountRaw = raw.total_amount
  if (!isRecord(amountRaw) || !Number.isInteger(amountRaw.value) || !isNonEmptyString(amountRaw.currency))
    fail('payload.total_amount must have an integer value and a non-empty currency')

  return {
    reservationId: raw.reservation_id,
    status: raw.status as ReservationStatus,
    guest: { id: guestRaw.id, email: guestRaw.email, name: guestRaw.name },
    propertyId: raw.property_id,
    checkIn: raw.check_in,
    checkOut: raw.check_out,
    totalAmount: { value: amountRaw.value as number, currency: amountRaw.currency },
  }
}

/**
 * Validates an unknown value against the wire contract (§2) and returns the typed event,
 * or throws NonRetryableError. This is pipeline step 1 ("parse + validate envelope"):
 * anything that fails here is dead-lettered immediately, never retried.
 */
export function parseReservationEvent(raw: unknown): ReservationEvent {
  if (!isRecord(raw)) fail('event must be an object')

  if (!isNonEmptyString(raw.event_id)) fail('event_id must be a non-empty string')
  if (!isNonEmptyString(raw.reservation_id)) fail('reservation_id must be a non-empty string')

  if (!isNonEmptyString(raw.type) || !EVENT_TYPES.has(raw.type))
    fail(`type must be one of reservation.created|updated|cancelled, got ${String(raw.type)}`)

  let version: number | null = null
  if (raw.version !== null && raw.version !== undefined) {
    if (!Number.isInteger(raw.version)) fail('version must be an integer or null')
    version = raw.version as number
  }

  if (!isNonEmptyString(raw.occurred_at) || Number.isNaN(Date.parse(raw.occurred_at)))
    fail('occurred_at must be a parseable ISO 8601 timestamp')

  const payload = parseSnapshot(raw.payload, raw.reservation_id)

  return {
    eventId: raw.event_id,
    reservationId: raw.reservation_id,
    type: raw.type as ReservationEventType,
    version,
    occurredAt: raw.occurred_at,
    payload,
  }
}
