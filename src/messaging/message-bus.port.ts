/**
 * Transport-agnostic message bus port.
 *
 * Both adapters (RabbitMQ, in-memory) implement the same semantics:
 *   at-least-once delivery, manual ack, capped retry with backoff, dead-lettering.
 *
 * This is a testability decision, not architectural decoration: the test suite runs
 * against the in-memory adapter without Docker, and it is the fallback if the broker
 * is unavailable.
 */

export const MESSAGE_BUS = Symbol('MESSAGE_BUS')

/** A message as handed to the consumer. */
export interface InboundMessage<T = unknown> {
  /** Broker-level delivery identity. NOT the domain event_id — deduplication is the
   *  consumer's job, because the broker cannot know two deliveries are the same event. */
  deliveryId: string
  payload: T
  /** 1-based. Incremented on every redelivery through the retry chain. */
  attempt: number
  headers: Record<string, unknown>
}

export type MessageHandler<T = unknown> = (msg: InboundMessage<T>) => Promise<void>

/**
 * Thrown by the handler when the failure is deterministic (malformed envelope, unknown
 * event type). Retrying a poison message is a busy loop, so it skips the retry chain
 * and is dead-lettered immediately.
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

export interface DeadLetter {
  deliveryId: string
  payload: unknown
  attempts: number
  reason: string
  failedAt: string
}

export interface BusMetrics {
  published: number
  delivered: number
  acked: number
  retried: number
  deadLettered: number
}

export interface MessageBus {
  publish<T>(payload: T, headers?: Record<string, unknown>): Promise<void>

  /** Single subscriber by design: FIFO per queue only holds with one consumer.
   *  Scaling requires partitioning by reservation_id — documented in the README. */
  subscribe<T>(handler: MessageHandler<T>): Promise<void>

  deadLetters(): Promise<DeadLetter[]>
  metrics(): Promise<BusMetrics>
}
