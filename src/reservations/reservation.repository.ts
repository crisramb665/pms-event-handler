import { Reservation } from './reservation.js'

/**
 * Purpose-built to Reservation, not a generic `Repository<T>` — CLAUDE.md rules out
 * generic-repository scaffolding, and there is exactly one aggregate here.
 */
export const RESERVATION_REPOSITORY = Symbol('RESERVATION_REPOSITORY')

export interface ReservationRepository {
  findById(reservationId: string): Promise<Reservation | null>
  save(reservation: Reservation): Promise<void>
  findAll(): Promise<Reservation[]>
}

export class InMemoryReservationRepository implements ReservationRepository {
  private readonly store = new Map<string, Reservation>()

  async findById(reservationId: string): Promise<Reservation | null> {
    return this.store.get(reservationId) ?? null
  }

  async save(reservation: Reservation): Promise<void> {
    this.store.set(reservation.reservationId, reservation)
  }

  async findAll(): Promise<Reservation[]> {
    return [...this.store.values()]
  }
}
