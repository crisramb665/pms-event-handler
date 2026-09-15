import { Injectable, Logger, OnApplicationShutdown, OnModuleInit } from '@nestjs/common'
import { ConfigService } from '@nestjs/config'
import { randomUUID } from 'node:crypto'
import { connect } from 'amqp-connection-manager'
import type { AmqpConnectionManager, ChannelWrapper } from 'amqp-connection-manager'
import type { ConfirmChannel, ConsumeMessage } from 'amqplib'
import {
  BusMetrics,
  DeadLetter,
  InboundMessage,
  MessageBus,
  MessageHandler,
  NonRetryableError,
} from './message-bus.port.js'
import { retryDelaysFromConfig } from './retry-delays.js'

// Topology names and routing key straight out of SPEC.md §6 — kept as constants,
// not config, because they are part of the contract, not deployment-specific.
const EXCHANGE = 'reservations'
const ROUTING_KEY = 'reservation.event'
const QUEUE_EVENTS = 'reservations.events'
const DLX = 'reservations.dlx'
const QUEUE_DLQ = 'reservations.dlq'
const HEADER_ATTEMPT = 'x-attempt'

interface RetryTier {
  queue: string
  ttlMs: number
}

// The queue name carries its TTL (reservations.retry.5s / .30s with the defaults, exactly
// as in §6). RabbitMQ refuses to redeclare an existing queue with different arguments
// (PRECONDITION_FAILED), so a changed RETRY_DELAYS_MS gets fresh queues instead of a
// boot failure against a broker that still has the old ones.
function retryTier(ttlMs: number): RetryTier {
  const label = ttlMs % 1000 === 0 ? `${ttlMs / 1000}s` : `${ttlMs}ms`
  return { queue: `reservations.retry.${label}`, ttlMs }
}

/**
 * RabbitMQ adapter for MessageBus, built directly on amqp-connection-manager +
 * amqplib — not Nest's built-in RMQ transport, which would hide exactly what this
 * exercise demonstrates: topology declaration, manual ack, prefetch, the retry chain.
 */
@Injectable()
export class RabbitMqBus implements MessageBus, OnModuleInit, OnApplicationShutdown {
  private readonly logger = new Logger(RabbitMqBus.name)

  private connection!: AmqpConnectionManager
  private channelWrapper!: ChannelWrapper
  private handler?: MessageHandler<any>
  private inFlight: Promise<void> = Promise.resolve()
  private shuttingDown = false

  private readonly deadLetterMirror: DeadLetter[] = []
  private counters: BusMetrics = {
    published: 0,
    delivered: 0,
    acked: 0,
    retried: 0,
    deadLettered: 0,
  }

  private readonly retryTiers: RetryTier[]

  constructor(private readonly config: ConfigService) {
    this.retryTiers = retryDelaysFromConfig(config).map(retryTier)
  }

  /** Attempts = tiers + 1: 3 with the default two tiers, matching §5. Same derivation as InMemoryBus. */
  private get maxAttempts(): number {
    return this.retryTiers.length + 1
  }

  async onModuleInit(): Promise<void> {
    // Default vhost is "/reservations" per docker-compose.yml — the leading slash is
    // part of the vhost name, so it must be percent-encoded again in the URI path.
    const url = this.config.get<string>('RABBITMQ_URL', 'amqp://guest:guest@localhost:5672/%2Freservations')

    this.connection = connect(url, { heartbeatIntervalInSeconds: 5 })
    this.connection.on('connect', () => this.logger.log('connected to RabbitMQ'))
    this.connection.on('disconnect', ({ err }) =>
      this.logger.warn(`disconnected from RabbitMQ: ${err?.message ?? 'unknown reason'}`),
    )

    // Bounded wait for the first connection; reconnection keeps retrying in the
    // background regardless, so a timeout here does not abort startup.
    await this.connection.connect({ timeout: 10_000 }).catch((error: Error) => {
      this.logger.error(`initial RabbitMQ connect failed, retrying in background: ${error.message}`)
    })

    this.channelWrapper = this.connection.createChannel({
      name: 'reservations-bus',
      confirm: true, // publisher confirms: publish() only resolves once the broker has durably accepted the message
      setup: (channel: ConfirmChannel) => this.declareTopology(channel),
    })

    await this.channelWrapper.waitForConnect()
  }

  /** Re-run on every (re)connect by amqp-connection-manager, so it must be idempotent. */
  private async declareTopology(channel: ConfirmChannel): Promise<void> {
    await channel.assertExchange(EXCHANGE, 'direct', { durable: true })
    await channel.assertExchange(DLX, 'direct', { durable: true })

    await channel.assertQueue(QUEUE_DLQ, { durable: true })
    await channel.bindQueue(QUEUE_DLQ, DLX, ROUTING_KEY)

    await channel.assertQueue(QUEUE_EVENTS, {
      durable: true,
      arguments: { 'x-dead-letter-exchange': DLX },
    })
    await channel.bindQueue(QUEUE_EVENTS, EXCHANGE, ROUTING_KEY)

    // TTL is set per queue, never per message: a queue with heterogeneous per-message
    // TTLs only expires from the head, so a 30s message would block the 5s ones behind it.
    for (const tier of this.retryTiers) {
      await channel.assertQueue(tier.queue, {
        durable: true,
        arguments: {
          'x-message-ttl': tier.ttlMs,
          'x-dead-letter-exchange': EXCHANGE,
          'x-dead-letter-routing-key': ROUTING_KEY,
        },
      })
    }
  }

  async publish<T>(payload: T, headers: Record<string, unknown> = {}): Promise<void> {
    const content = Buffer.from(JSON.stringify(payload))
    await this.channelWrapper.publish(EXCHANGE, ROUTING_KEY, content, {
      persistent: true,
      contentType: 'application/json',
      messageId: randomUUID(),
      headers: { [HEADER_ATTEMPT]: 1, ...headers },
    })
    this.counters.published += 1
  }

  /** Single subscriber by design: FIFO per queue only holds with one consumer. */
  async subscribe<T>(handler: MessageHandler<T>): Promise<void> {
    if (this.handler) throw new Error('RabbitMqBus supports a single subscriber (FIFO guarantee).')

    this.handler = handler

    await this.channelWrapper.consume(QUEUE_EVENTS, this.consumeMessage, {
      prefetch: 1,
      noAck: false,
      consumerTag: 'reservations-events-consumer',
    })
  }

  async deadLetters(): Promise<DeadLetter[]> {
    return [...this.deadLetterMirror]
  }

  async metrics(): Promise<BusMetrics> {
    return { ...this.counters }
  }

  // ---------------------------------------------------------------------------

  /**
   * amqp-connection-manager's Consumer.onMessage is fire-and-forget from its own
   * perspective; capturing the promise in `inFlight` is what lets onApplicationShutdown
   * wait for the message currently being processed instead of tearing the channel down
   * under it. Overlap across deliveries cannot happen anyway: prefetch(1) means the
   * broker withholds the next message until this one is acked.
   */
  private readonly consumeMessage = (msg: ConsumeMessage): void => {
    this.inFlight = this.handleDelivery(msg).catch((error) => {
      this.logger.error(`unhandled error processing delivery: ${String(error)}`)
    })
  }

  private async handleDelivery(msg: ConsumeMessage): Promise<void> {
    const attempt = this.readAttempt(msg)
    this.counters.delivered += 1

    let payload: unknown
    try {
      payload = JSON.parse(msg.content.toString('utf8'))
    } catch (error) {
      // Not valid JSON at all — the handler can't even be called with a payload.
      // Equivalent to step 1 of the pipeline ("invalid? -> DLQ immediately, no retry").
      await this.deadLetter(msg, attempt, `malformed payload: ${(error as Error).message}`)
      return
    }

    const inbound: InboundMessage = {
      deliveryId: String(msg.properties.messageId ?? `dt-${msg.fields.deliveryTag}`),
      payload,
      attempt,
      headers: { ...msg.properties.headers },
    }

    try {
      await this.handler!(inbound)
      this.channelWrapper.ack(msg)
      this.counters.acked += 1
    } catch (error) {
      await this.handleFailure(msg, attempt, error)
    }
  }

  private async handleFailure(msg: ConsumeMessage, attempt: number, error: unknown): Promise<void> {
    const reason = error instanceof Error ? error.message : String(error)

    if (error instanceof NonRetryableError) {
      await this.deadLetter(msg, attempt, `non-retryable: ${reason}`)
      return
    }

    if (attempt >= this.maxAttempts) {
      await this.deadLetter(msg, attempt, `attempts exhausted: ${reason}`)
      return
    }

    const tier = this.retryTiers[attempt - 1]
    this.logger.warn(
      `retry scheduled delivery=${String(msg.properties.messageId)} attempt=${attempt} tier=${tier.queue} reason=${reason}`,
    )

    // Publish-then-ack: the retry copy must be durably queued before the original is
    // relinquished, or a crash between the two silently loses the message. The reverse
    // order can double-deliver instead, which the idempotent consumer already tolerates.
    await this.channelWrapper.sendToQueue(tier.queue, msg.content, {
      persistent: true,
      contentType: msg.properties.contentType,
      messageId: msg.properties.messageId,
      headers: { ...msg.properties.headers, [HEADER_ATTEMPT]: attempt + 1 },
    })
    this.channelWrapper.ack(msg)
    this.counters.retried += 1
  }

  private async deadLetter(msg: ConsumeMessage, attempt: number, reason: string): Promise<void> {
    await this.channelWrapper.publish(DLX, ROUTING_KEY, msg.content, {
      persistent: true,
      contentType: msg.properties.contentType,
      messageId: msg.properties.messageId,
      headers: { ...msg.properties.headers, [HEADER_ATTEMPT]: attempt },
    })
    this.channelWrapper.ack(msg)
    this.counters.deadLettered += 1

    // In-memory mirror instead of reading reservations.dlq back with a second, `get`-mode
    // channel: this process is the only writer to the DLQ, so the mirror cannot drift, and
    // it avoids holding a second channel open purely to serve GET /dlq. Trade-off: the
    // mirror resets on restart, unlike the DLQ queue itself, which is durable.
    let payload: unknown
    try {
      payload = JSON.parse(msg.content.toString('utf8'))
    } catch {
      payload = msg.content.toString('utf8')
    }
    this.deadLetterMirror.push({
      deliveryId: String(msg.properties.messageId ?? `dt-${msg.fields.deliveryTag}`),
      payload,
      attempts: attempt,
      reason,
      failedAt: new Date().toISOString(),
    })
    this.logger.error(`dead-lettered delivery=${String(msg.properties.messageId)} reason=${reason}`)
  }

  private readAttempt(msg: ConsumeMessage): number {
    const raw = msg.properties.headers?.[HEADER_ATTEMPT]
    const attempt = typeof raw === 'number' ? raw : Number(raw)
    return Number.isFinite(attempt) && attempt >= 1 ? attempt : 1
  }

  async onApplicationShutdown(): Promise<void> {
    if (this.shuttingDown) return
    this.shuttingDown = true

    await this.channelWrapper
      ?.cancelAll()
      .catch((error: unknown) => this.logger.warn(`cancel consumers failed: ${String(error)}`))

    // Let the delivery currently mid-processing finish (ack, retry, or DLQ) before
    // closing the channel — closing under it would leave that message unacked.
    await this.inFlight

    await this.channelWrapper?.close().catch(() => undefined)
    await this.connection?.close().catch(() => undefined)
  }
}
