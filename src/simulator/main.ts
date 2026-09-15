import { NestFactory } from '@nestjs/core'
import { ConfigService } from '@nestjs/config'
import { AppModule } from '../app.module.js'
import { MESSAGE_BUS } from '../messaging/message-bus.port.js'
import type { MessageBus } from '../messaging/message-bus.port.js'
import { retryDelaysFromConfig } from '../messaging/retry-delays.js'
import { ReservationEventConsumer } from '../events/reservation-event.consumer.js'
import { RESERVATION_REPOSITORY } from '../reservations/reservation.repository.js'
import type { ReservationRepository } from '../reservations/reservation.repository.js'
import { EMAIL_SENDER, SimulatedEmailSender } from '../notifications/email-sender.js'
import type { EmailSender } from '../notifications/email-sender.js'
import { SCENARIOS } from './feed.simulator.js'
import type { Scenario } from './feed.simulator.js'

interface Snapshot {
  emailsSent: number
  deadLettered: number
}

interface Row {
  scenario: string
  'expected state': string
  'actual state': string
  'expected emails': number
  'actual emails': number
  'expected dlq': number
  'actual dlq': number
  result: 'PASS' | 'FAIL'
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

/**
 * Idle = nothing published is still unresolved, and the counters have not moved for a
 * short quiet window. "Unresolved" is published − acked − deadLettered: a message parked
 * in a retry queue is neither, so this cannot declare idle while a retry is pending —
 * which is what makes scenario 8 wait through both tiers instead of racing them.
 */
async function waitForIdle(bus: MessageBus, consumer: ReservationEventConsumer, timeoutMs: number): Promise<void> {
  const quietWindowMs = 300
  const deadline = Date.now() + timeoutMs
  let last = ''
  let quietSince = Date.now()

  for (;;) {
    const metrics = await bus.metrics()
    const unresolved = metrics.published - metrics.acked - metrics.deadLettered
    const current = JSON.stringify({ metrics, counters: consumer.getCounters() })
    if (current !== last) {
      last = current
      quietSince = Date.now()
    }
    if (unresolved <= 0 && Date.now() - quietSince >= quietWindowMs) return
    if (Date.now() > deadline) {
      throw new Error(`timed out after ${timeoutMs}ms waiting for idle (unresolved=${unresolved})`)
    }
    await sleep(100)
  }
}

async function snapshot(bus: MessageBus, consumer: ReservationEventConsumer): Promise<Snapshot> {
  return { emailsSent: consumer.getCounters().emailsSent, deadLettered: (await bus.metrics()).deadLettered }
}

/**
 * RabbitMqBus keeps reconnecting forever when the broker is down — right for the service,
 * wrong for a one-shot tool, which would just hang with no output. Bound the boot instead.
 */
async function boot(timeoutMs: number) {
  let timer: NodeJS.Timeout
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () =>
        reject(
          new Error(
            `could not boot within ${timeoutMs}ms — is RabbitMQ up? (docker compose up -d, or MESSAGE_BUS_DRIVER=memory)`,
          ),
        ),
      timeoutMs,
    )
  })
  return Promise.race([NestFactory.createApplicationContext(AppModule), timeout]).finally(() => clearTimeout(timer))
}

async function main(): Promise<void> {
  const app = await boot(20_000)

  const bus = app.get<MessageBus>(MESSAGE_BUS)
  const consumer = app.get(ReservationEventConsumer)
  const reservations = app.get<ReservationRepository>(RESERVATION_REPOSITORY)
  const sender = app.get<EmailSender>(EMAIL_SENDER)

  // The simulator never changes RETRY_DELAYS_MS; it only sizes its own patience by it, so
  // scenario 8 (two retries) has room to finish under the real 5s + 30s schedule.
  const retryDelays = retryDelaysFromConfig(app.get(ConfigService))
  const idleTimeoutMs = retryDelays.reduce((sum, ms) => sum + ms, 0) + 15_000

  const rows: Row[] = []

  for (const scenario of SCENARIOS) {
    console.log(`\n=== ${scenario.name} ===\n${scenario.description}\n`)

    if (scenario.failEmailSends) {
      // Simulator-specific hook: the production EmailSender port has no such knob.
      if (!(sender instanceof SimulatedEmailSender)) {
        throw new Error(`${scenario.name} needs SimulatedEmailSender, got ${sender.constructor.name}`)
      }
      sender.failNextN(scenario.failEmailSends)
    }

    const before = await snapshot(bus, consumer)

    if (scenario.concurrent) {
      await Promise.all(scenario.events.map((event) => bus.publish(event)))
    } else {
      for (const event of scenario.events) await bus.publish(event)
    }

    await waitForIdle(bus, consumer, idleTimeoutMs)

    const after = await snapshot(bus, consumer)
    rows.push(await evaluate(scenario, reservations, before, after))
  }

  console.log('\n')
  console.table(rows)

  const failures = rows.filter((row) => row.result === 'FAIL').length
  console.log(failures === 0 ? '\nall scenarios passed' : `\n${failures} scenario(s) FAILED`)

  await app.close()
  process.exitCode = failures === 0 ? 0 : 1
}

async function evaluate(
  scenario: Scenario,
  reservations: ReservationRepository,
  before: Snapshot,
  after: Snapshot,
): Promise<Row> {
  const reservation = await reservations.findById(scenario.reservationId)
  const actualStatus = reservation?.status ?? null
  const actualEmails = after.emailsSent - before.emailsSent
  const actualDlq = after.deadLettered - before.deadLettered
  const expectedDlq = scenario.expected.deadLetters ?? 0

  const pass =
    actualStatus === scenario.expected.status &&
    actualEmails === scenario.expected.emails &&
    actualDlq === expectedDlq

  return {
    scenario: scenario.name,
    'expected state': String(scenario.expected.status),
    'actual state': String(actualStatus),
    'expected emails': scenario.expected.emails,
    'actual emails': actualEmails,
    'expected dlq': expectedDlq,
    'actual dlq': actualDlq,
    result: pass ? 'PASS' : 'FAIL',
  }
}

main().catch((error) => {
  console.error('simulator failed:', error)
  process.exit(1)
})
