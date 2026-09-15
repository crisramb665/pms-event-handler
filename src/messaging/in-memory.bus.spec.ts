import { InMemoryBus } from './in-memory.bus.js'
import { NonRetryableError } from './message-bus.port.js'

/**
 * These assert the BUS CONTRACT, not the bus implementation. When the RabbitMQ adapter
 * lands, the same suite should pass against it (integration-tagged), which is what makes
 * the port meaningful rather than decorative.
 */
describe('InMemoryBus', () => {
  const fastRetries = { retryDelaysMs: [10, 20] } // maxAttempts = 3

  it('retries a transient failure and eventually acks', async () => {
    const bus = new InMemoryBus(fastRetries)
    const attempts: number[] = []

    await bus.subscribe(async (msg) => {
      attempts.push(msg.attempt)
      if (msg.attempt < 3) throw new Error('transient')
    })

    await bus.publish({ hello: 'world' })
    await bus.waitForIdle()

    expect(attempts).toEqual([1, 2, 3])
    expect(await bus.deadLetters()).toHaveLength(0)
  })

  it('dead-letters after attempts are exhausted', async () => {
    const bus = new InMemoryBus(fastRetries)

    await bus.subscribe(async () => {
      throw new Error('always down')
    })

    await bus.publish({ id: 'evt_1' })
    await bus.waitForIdle()

    const dlq = await bus.deadLetters()
    expect(dlq).toHaveLength(1)
    expect(dlq[0].attempts).toBe(3)
    expect(dlq[0].reason).toContain('attempts exhausted')
  })

  it('dead-letters a poison message immediately, without retrying', async () => {
    const bus = new InMemoryBus(fastRetries)
    let calls = 0

    await bus.subscribe(async () => {
      calls += 1
      throw new NonRetryableError('malformed envelope')
    })

    await bus.publish({ garbage: true })
    await bus.waitForIdle()

    expect(calls).toBe(1) // retrying a poison message would be a busy loop
    expect((await bus.deadLetters())[0].reason).toContain('non-retryable')
  })

  it('a failing message does not block the ones behind it (I4)', async () => {
    const bus = new InMemoryBus(fastRetries)
    const processed: string[] = []

    await bus.subscribe(async (msg) => {
      const { id } = msg.payload as { id: string }
      if (id === 'poison') throw new NonRetryableError('bad')
      processed.push(id)
    })

    await bus.publish({ id: 'poison' })
    await bus.publish({ id: 'a' })
    await bus.publish({ id: 'b' })
    await bus.waitForIdle()

    expect(processed).toEqual(['a', 'b'])
    expect(await bus.deadLetters()).toHaveLength(1)
  })

  it('preserves FIFO order for successfully handled messages', async () => {
    const bus = new InMemoryBus(fastRetries)
    const seen: number[] = []

    await bus.subscribe(async (msg) => {
      await new Promise((r) => setTimeout(r, 1))
      seen.push((msg.payload as { n: number }).n)
    })

    for (let n = 1; n <= 5; n++) await bus.publish({ n })
    await bus.waitForIdle()

    expect(seen).toEqual([1, 2, 3, 4, 5])
  })
})
