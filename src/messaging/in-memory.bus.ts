import { Injectable, Logger, OnApplicationShutdown } from '@nestjs/common'
import { randomUUID } from 'node:crypto'
import {
  BusMetrics,
  DeadLetter,
  InboundMessage,
  MessageBus,
  MessageHandler,
  NonRetryableError,
} from './message-bus.port.js'

interface QueuedMessage {
  deliveryId: string
  payload: unknown
  headers: Record<string, unknown>
  attempt: number
}

export interface InMemoryBusOptions {
  /** One entry per retry tier. Mirrors the RabbitMQ retry queues (5s / 30s).
   *  maxAttempts is derived: retryDelaysMs.length + 1. Tests override with ms values. */
  retryDelaysMs?: number[]
}

/**
 * In-memory adapter that faithfully reproduces the broker semantics we rely on:
 * sequential consumption (equivalent to prefetch=1 + single consumer), manual ack,
 * capped retry with backoff, and dead-lettering.
 *
 * Deliberately NOT a naive "call the handler and hope" fake — if it did not model
 * redelivery, the tests would not exercise the idempotency this system exists to prove.
 */
@Injectable()
export class InMemoryBus implements MessageBus, OnApplicationShutdown {
  private readonly logger = new Logger(InMemoryBus.name)

  private readonly queue: QueuedMessage[] = []
  private readonly dlq: DeadLetter[] = []
  private readonly retryDelaysMs: number[]
  private readonly pendingRetries = new Set<NodeJS.Timeout>()

  private handler?: MessageHandler<any>
  private draining = false
  private stopped = false

  private counters: BusMetrics = {
    published: 0,
    delivered: 0,
    acked: 0,
    retried: 0,
    deadLettered: 0,
  }

  constructor(options: InMemoryBusOptions = {}) {
    this.retryDelaysMs = options.retryDelaysMs ?? [5_000, 30_000]
  }

  private get maxAttempts(): number {
    return this.retryDelaysMs.length + 1
  }

  async publish<T>(payload: T, headers: Record<string, unknown> = {}): Promise<void> {
    this.counters.published += 1
    this.enqueue({ deliveryId: randomUUID(), payload, headers, attempt: 1 })
  }

  async subscribe<T>(handler: MessageHandler<T>): Promise<void> {
    if (this.handler) throw new Error('InMemoryBus supports a single subscriber (FIFO guarantee).')

    this.handler = handler
    void this.drain()
  }

  async deadLetters(): Promise<DeadLetter[]> {
    return [...this.dlq]
  }

  async metrics(): Promise<BusMetrics> {
    return { ...this.counters }
  }

  /**
   * Test affordance: resolves when the queue is empty, no retry is scheduled, and the
   * consumer is idle. Without this, tests would race the retry timers.
   */
  async waitForIdle(timeoutMs = 5_000): Promise<void> {
    const deadline = Date.now() + timeoutMs
    while (this.queue.length > 0 || this.pendingRetries.size > 0 || this.draining) {
      if (Date.now() > deadline) throw new Error('InMemoryBus.waitForIdle timed out')

      await new Promise((r) => setTimeout(r, 5))
    }
  }

  onApplicationShutdown(): void {
    this.stopped = true
    for (const timer of this.pendingRetries) clearTimeout(timer)
    this.pendingRetries.clear()
  }

  // ---------------------------------------------------------------------------

  private enqueue(message: QueuedMessage): void {
    this.queue.push(message)
    void this.drain()
  }

  /**
   * Sequential consumer loop. One message in flight at a time — this is what makes
   * per-queue FIFO hold, and it mirrors prefetch=1 on the Rabbit side.
   */
  private async drain(): Promise<void> {
    if (this.draining || !this.handler || this.stopped) return
    this.draining = true

    try {
      while (this.queue.length > 0 && !this.stopped) {
        const message = this.queue.shift()!
        await this.deliver(message)
      }
    } finally {
      this.draining = false
    }
  }

  private async deliver(message: QueuedMessage): Promise<void> {
    const inbound: InboundMessage = {
      deliveryId: message.deliveryId,
      payload: message.payload,
      attempt: message.attempt,
      headers: message.headers,
    }

    this.counters.delivered += 1

    try {
      await this.handler!(inbound)
      this.counters.acked += 1
    } catch (error) {
      this.handleFailure(message, error)
    }
  }

  private handleFailure(message: QueuedMessage, error: unknown): void {
    const reason = error instanceof Error ? error.message : String(error)

    // Deterministic failure: retrying cannot help.
    if (error instanceof NonRetryableError) {
      this.deadLetter(message, `non-retryable: ${reason}`)
      return
    }

    if (message.attempt >= this.maxAttempts) {
      this.deadLetter(message, `attempts exhausted: ${reason}`)
      return
    }

    const delay = this.retryDelaysMs[message.attempt - 1]
    this.counters.retried += 1
    this.logger.warn(
      `retry scheduled delivery=${message.deliveryId} attempt=${message.attempt} in=${delay}ms reason=${reason}`,
    )

    // Equivalent to republishing into the TTL'd retry queue: the message leaves the
    // main queue immediately so it never blocks the messages behind it.
    const timer = setTimeout(() => {
      this.pendingRetries.delete(timer)
      if (this.stopped) return
      this.enqueue({ ...message, attempt: message.attempt + 1 })
    }, delay)

    this.pendingRetries.add(timer)
  }

  private deadLetter(message: QueuedMessage, reason: string): void {
    this.counters.deadLettered += 1
    this.dlq.push({
      deliveryId: message.deliveryId,
      payload: message.payload,
      attempts: message.attempt,
      reason,
      failedAt: new Date().toISOString(),
    })
    this.logger.error(`dead-lettered delivery=${message.deliveryId} reason=${reason}`)
  }
}
