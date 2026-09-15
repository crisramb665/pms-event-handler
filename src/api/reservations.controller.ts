import { Controller, Get, Inject, NotFoundException, Param } from '@nestjs/common'
import { MESSAGE_BUS } from '../messaging/message-bus.port.js'
import type { MessageBus } from '../messaging/message-bus.port.js'
import { RESERVATION_REPOSITORY } from '../reservations/reservation.repository.js'
import type { ReservationRepository } from '../reservations/reservation.repository.js'
import { EMAIL_LEDGER } from '../notifications/email-ledger.js'
import type { EmailLedger } from '../notifications/email-ledger.js'
import { ReservationEventConsumer } from '../events/reservation-event.consumer.js'

/**
 * Inspection surface for §10, not a public API: it returns the in-memory domain objects
 * as they are — no DTOs, no mappers, no pagination. Its job is to make the ordering and
 * dedup decisions visible (the timeline above all) rather than to present a stable contract.
 * No logic lives here; every value comes straight from a repository, ledger, bus or counter.
 */
@Controller()
export class ReservationsController {
  constructor(
    @Inject(RESERVATION_REPOSITORY) private readonly reservations: ReservationRepository,
    @Inject(EMAIL_LEDGER) private readonly emailLedger: EmailLedger,
    @Inject(MESSAGE_BUS) private readonly bus: MessageBus,
    private readonly consumer: ReservationEventConsumer,
  ) {}

  @Get('reservations')
  async list() {
    return this.reservations.findAll()
  }

  @Get('reservations/:id')
  async one(@Param('id') id: string) {
    const reservation = await this.reservations.findById(id)
    if (!reservation) throw new NotFoundException(`reservation ${id} not found`)

    // The aggregate deliberately does not store email state (§1: state and effects are
    // decoupled); the ledger is the source of truth, so it is joined in here for display.
    return { ...reservation, emailStatus: (await this.emailLedger.getState(id)) ?? null }
  }

  @Get('reservations/:id/timeline')
  async timeline(@Param('id') id: string) {
    const reservation = await this.reservations.findById(id)
    if (!reservation) throw new NotFoundException(`reservation ${id} not found`)
    return reservation.timeline
  }

  @Get('dlq')
  async deadLetters() {
    return this.bus.deadLetters()
  }

  @Get('metrics')
  async metrics() {
    return {
      consumer: this.consumer.getCounters(),
      bus: await this.bus.metrics(),
      emailsPendingReservations: await this.emailLedger.pendingReservations(),
    }
  }
}
