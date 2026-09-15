/**
 * Dedup store keyed by `event_id` — pipeline step 2, invariant I1: processing the same
 * event_id N times leaves the same state as processing it once. Deliberately separate from
 * the ordering guard (ordering-key.ts): this catches literal redelivery of the same event,
 * the ordering guard catches distinct-but-stale events.
 */
export const PROCESSED_EVENTS_STORE = Symbol('PROCESSED_EVENTS_STORE')

export interface ProcessedEventsStore {
  has(eventId: string): Promise<boolean>
  markProcessed(eventId: string): Promise<void>
}

export class InMemoryProcessedEventsStore implements ProcessedEventsStore {
  // Unbounded growth is a conscious choice for this exercise, not an oversight — §7 notes
  // it explicitly. A production deployment needs a TTL'd table or a Redis set with expiry;
  // an in-memory Set that never shrinks is fine for a time-boxed demo, not for a long-lived
  // process with a large reservation volume.
  private readonly seen = new Set<string>()

  async has(eventId: string): Promise<boolean> {
    return this.seen.has(eventId)
  }

  async markProcessed(eventId: string): Promise<void> {
    this.seen.add(eventId)
  }
}
