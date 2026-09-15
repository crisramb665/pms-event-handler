/**
 * The ordering key a single event is compared by. `arrivalSeq` is not part of the wire
 * event — it is assigned by the consumer as each event is dequeued (a process-lifetime,
 * ever-increasing counter), and only matters as a last-resort tiebreaker.
 */
export interface OrderingKey {
  version: number | null
  occurredAt: string
  arrivalSeq: number
}

/**
 * A cascade, not a fixed field, because real PMS webhooks vary in what ordering guarantee
 * they actually expose: some give a monotonic `version` per reservation, most only a
 * timestamp that two events can share down to the millisecond. Trying the strongest signal
 * first and falling back means the system degrades gracefully instead of breaking when the
 * source turns out weaker than assumed — and isolating the cascade here means swapping the
 * source's guarantee later touches this one file, not every call site (§2).
 *
 * Returns <0 if `a` is older than `b`, >0 if newer, 0 if equal.
 */
export function compareOrderingKeys(a: OrderingKey, b: OrderingKey): number {
  if (a.version !== null && b.version !== null) return a.version - b.version

  const byTime = Date.parse(a.occurredAt) - Date.parse(b.occurredAt)
  if (byTime !== 0) return byTime

  return a.arrivalSeq - b.arrivalSeq
}
