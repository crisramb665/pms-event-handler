import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common'
import { MESSAGE_BUS } from '../messaging/message-bus.port.js'
import type { InboundMessage, MessageBus } from '../messaging/message-bus.port.js'
import { KeyedMutex } from '../common/keyed-mutex.js'
import { parseReservationEvent } from './reservation-event.js'
import type { ReservationEvent } from './reservation-event.js'
import { PROCESSED_EVENTS_STORE } from './processed-events.store.js'
import type { ProcessedEventsStore } from './processed-events.store.js'
import { RESERVATION_REPOSITORY } from '../reservations/reservation.repository.js'
import type { ReservationRepository } from '../reservations/reservation.repository.js'
import { applyTransition, crossedIntoConfirmed } from '../reservations/reservation.js'
import type { Reservation } from '../reservations/reservation.js'
import { EMAIL_LEDGER } from '../notifications/email-ledger.js'
import type { EmailLedger } from '../notifications/email-ledger.js'
import { EMAIL_SENDER } from '../notifications/email-sender.js'
import type { EmailSender } from '../notifications/email-sender.js'

export interface Counters {
  received: number
  deduplicated: number
  stale: number
  rejected: number
  applied: number
  emailsSent: number
  emailsSkipped: number
}

/**
 * Orchestrates the pipeline in §5, step by step. This file makes no domain decisions of
 * its own — parsing, ordering, the state machine, and email idempotency all live in the
 * modules it calls. It only sequences them, holds the lock, and counts what happened.
 */
@Injectable()
export class ReservationEventConsumer implements OnModuleInit {
  private readonly logger = new Logger(ReservationEventConsumer.name)
  private readonly lock = new KeyedMutex()

  // Post-dedupe, ever-increasing — the ordering cascade's last-resort tiebreaker
  // (ordering-key.ts). Incremented once per event actually reaching applyTransition,
  // never for a message dropped earlier as a duplicate.
  private arrivalSeq = 0

  private readonly counters: Counters = {
    received: 0,
    deduplicated: 0,
    stale: 0,
    rejected: 0,
    applied: 0,
    emailsSent: 0,
    emailsSkipped: 0,
  }

  constructor(
    @Inject(MESSAGE_BUS) private readonly bus: MessageBus,
    @Inject(RESERVATION_REPOSITORY) private readonly reservations: ReservationRepository,
    @Inject(PROCESSED_EVENTS_STORE) private readonly processedEvents: ProcessedEventsStore,
    @Inject(EMAIL_LEDGER) private readonly emailLedger: EmailLedger,
    @Inject(EMAIL_SENDER) private readonly emailSender: EmailSender,
  ) {}

  async onModuleInit(): Promise<void> {
    await this.bus.subscribe(this.handleMessage)
  }

  getCounters(): Counters {
    return { ...this.counters }
  }

  private readonly handleMessage = async (msg: InboundMessage): Promise<void> => {
    this.counters.received += 1

    // Step 1: invalid envelope is a NonRetryableError — left to propagate uncaught, so
    // the bus dead-letters it immediately instead of retrying a poison message.
    const event = parseReservationEvent(msg.payload)

    // Step 2 (I1): a literal redelivery of an event we already processed is dropped
    // before the lock, before applyTransition ever sees it.
    if (await this.processedEvents.has(event.eventId)) {
      this.counters.deduplicated += 1
      this.logger.log(`event_id=${event.eventId} reservation_id=${event.reservationId} outcome=deduplicated`)
      return
    }

    // Steps 3-10: everything from here on happens under the per-reservation lock.
    await this.lock.runExclusive(event.reservationId, () => this.processUnderLock(event))
  }

  private async processUnderLock(event: ReservationEvent): Promise<void> {
    // Re-checked inside the lock: the fast-path check in handleMessage is an optimization,
    // not a guarantee. Two concurrent deliveries of the same event_id can both pass it
    // before either marks the event processed. Only this one is serialized against
    // markProcessed below, so only this one actually enforces I1.
    if (await this.processedEvents.has(event.eventId)) {
      this.counters.deduplicated += 1
      return
    }

    const current = await this.reservations.findById(event.reservationId)

    this.arrivalSeq += 1
    const result = applyTransition(current, event, this.arrivalSeq)

    this.countOutcome(result.outcome)
    this.logger.log(
      `event_id=${event.eventId} reservation_id=${event.reservationId} outcome=${result.outcome}` +
        (result.reason ? ` reason=${result.reason}` : ''),
    )

    if (result.outcome === 'applied' && crossedIntoConfirmed(current, result.reservation)) {
      // Deliberately still inside the lock and before the reservation is saved: if
      // send() fails we re-throw, so neither the state change nor the email are
      // considered "done" for this event, and the whole message is retried together
      // rather than leaving a reservation that reads CONFIRMED while its guest was
      // never actually notified.
      await this.sendConfirmationEmail(event, result.reservation)
    }

    // Persisted for every outcome, including stale/rejected: the timeline changed even
    // when the business state did not (§10 — the timeline endpoint needs the full record).
    await this.reservations.save(result.reservation)

    // Marked at the very end, never before: if anything above throws, this event_id must
    // still look "unprocessed" on redelivery, or the dedupe check would swallow the retry.
    await this.processedEvents.markProcessed(event.eventId)
  }

  private async sendConfirmationEmail(event: ReservationEvent, reservation: Reservation): Promise<void> {
    const reserved = await this.emailLedger.reserve(reservation.reservationId)
    if (!reserved) {
      // Already reserved or sent earlier in this reservation's lifetime — e.g. confirmed,
      // cancelled, and confirmed again (§3: one email per reservation, not per crossing).
      this.counters.emailsSkipped += 1
      this.logger.log(`event_id=${event.eventId} reservation_id=${reservation.reservationId} outcome=email-skipped`)
      return
    }

    try {
      await this.emailSender.sendConfirmation({
        reservationId: reservation.reservationId,
        guestEmail: reservation.guest.email,
        guestName: reservation.guest.name,
      })
    } catch (error) {
      // reserve() succeeded, send() did not: release so a retry of this same event can
      // reserve and send again, instead of permanently believing the intent is claimed.
      await this.emailLedger.release(reservation.reservationId)
      this.logger.warn(
        `event_id=${event.eventId} reservation_id=${reservation.reservationId} outcome=email-send-failed reason=${String(error)}`,
      )
      throw error // propagates to the bus, which retries the whole message
    }

    await this.emailLedger.markSent(reservation.reservationId)
    this.counters.emailsSent += 1
    this.logger.log(`event_id=${event.eventId} reservation_id=${reservation.reservationId} outcome=email-sent`)
  }

  private countOutcome(outcome: 'applied' | 'stale' | 'rejected'): void {
    this.counters[outcome] += 1
  }
}
