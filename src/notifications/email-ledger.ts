export type EmailLedgerState = 'reserved' | 'sent'

/**
 * Idempotent effect ledger for the confirmation email, keyed by `reservation_id` — the
 * effect's own identity — not by `event_id`. The email must go out once per reservation
 * for its lifetime (§3), not once per event that happens to cross into CONFIRMED.
 *
 * Call order is always reserve -> send -> markSent, never the reverse. If the process
 * crashes between reserve and send, the reservation is left "reserved but unsent" —
 * recoverable, because nothing was actually sent yet. If send happened first and the crash
 * landed before recording it, the email is already out but nothing durable says so, and a
 * retry double-sends it. Under this ordering a crash can produce a missing email; it can
 * never produce a duplicate. That trade is deliberate (§5, "The email is the only
 * irreversible step") — a missing email can always be retried, a duplicate cannot be unsent.
 */

//  * Call order is always reserve -> send -> markSent, never the reverse. If the process
//  * crashes between reserve and send, the entry is left in 'reserved' and this ledger will
//  * never release it on its own: a later attempt sees an existing entry and refuses to
//  * send. That is deliberate — a stuck 'reserved' is a *visible* missing email (surfaced by
//  * getState and by GET /reservations/:id), whereas auto-releasing it would reopen the
//  * duplicate window this ledger exists to close. Recovery is explicit, not automatic.
//  *
//  * In production the entry would carry a TTL and a reconciliation job would sweep stale
//  * reservations, turning "explicit recovery" into "eventual recovery" without weakening
//  * the guarantee. Out of scope here (README, "scaling to production").
export interface EmailLedger {
  /** True if this call reserved the intent; false if one already existed (reserved or
   *  sent) — in that case the caller must not send. */
  reserve(reservationId: string): Promise<boolean>
  markSent(reservationId: string): Promise<void>
  getState(reservationId: string): Promise<EmailLedgerState | undefined>
  /** Entries stuck in 'reserved' — a crash between reserve and send. Exposed so a missing
   *  email is observable rather than silent. */
  pendingReservations(): Promise<string[]>
}

export class InMemoryEmailLedger implements EmailLedger {
  private readonly entries = new Map<string, EmailLedgerState>()

  async reserve(reservationId: string): Promise<boolean> {
    if (this.entries.has(reservationId)) return false
    this.entries.set(reservationId, 'reserved')
    return true
  }

  async markSent(reservationId: string): Promise<void> {
    this.entries.set(reservationId, 'sent')
  }

  async getState(reservationId: string): Promise<EmailLedgerState | undefined> {
    return this.entries.get(reservationId)
  }

  async pendingReservations(): Promise<string[]> {
    return [...this.entries.entries()].filter(([, state]) => state === 'reserved').map(([id]) => id)
  }
}
