import { Module } from '@nestjs/common'
import { ConfigModule } from '@nestjs/config'
import { MessagingModule } from './messaging/messaging.module.js'
import { ReservationEventConsumer } from './events/reservation-event.consumer.js'
import { InMemoryProcessedEventsStore, PROCESSED_EVENTS_STORE } from './events/processed-events.store.js'
import { InMemoryReservationRepository, RESERVATION_REPOSITORY } from './reservations/reservation.repository.js'
import { EMAIL_LEDGER, InMemoryEmailLedger } from './notifications/email-ledger.js'
import { EMAIL_SENDER, SimulatedEmailSender } from './notifications/email-sender.js'
import { ReservationsController } from './api/reservations.controller.js'

// Ports are bound to their in-memory adapters right here rather than in one module per
// folder: there is exactly one binding for each, and a module apiece would be scaffolding.
@Module({
  imports: [ConfigModule.forRoot({ isGlobal: true }), MessagingModule],
  controllers: [ReservationsController],
  providers: [
    { provide: RESERVATION_REPOSITORY, useClass: InMemoryReservationRepository },
    { provide: PROCESSED_EVENTS_STORE, useClass: InMemoryProcessedEventsStore },
    { provide: EMAIL_LEDGER, useClass: InMemoryEmailLedger },
    { provide: EMAIL_SENDER, useClass: SimulatedEmailSender },
    ReservationEventConsumer,
  ],
})
export class AppModule {}
