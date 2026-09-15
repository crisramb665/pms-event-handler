/**
 * Per-key mutual exclusion via a Map of chained promises — the per-reservation lock from
 * §6, kept even with a single consumer as the safety net for raising concurrency later.
 * No external locking library: a Map of promise chains is enough to serialize *async* work
 * (findById -> applyTransition -> save) per key without blocking unrelated keys.
 */
export class KeyedMutex {
  private readonly tails = new Map<string, Promise<void>>()

  async runExclusive<T>(key: string, fn: () => Promise<T>): Promise<T> {
    const previousTail = this.tails.get(key) ?? Promise.resolve()
    let resolveTail!: () => void
    const tail = new Promise<void>((resolve) => {
      resolveTail = resolve
    })
    const chained = previousTail.then(() => tail)
    this.tails.set(key, chained)

    await previousTail
    try {
      return await fn()
    } finally {
      resolveTail()
      // Only the last-queued caller for this key clears the map entry — an earlier one
      // finishing first would otherwise delete a slot a later caller is still waiting on.
      if (this.tails.get(key) === chained) {
        this.tails.delete(key)
      }
    }
  }
}
