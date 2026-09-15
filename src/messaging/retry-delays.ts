import { ConfigService } from '@nestjs/config'

export const DEFAULT_RETRY_DELAYS_MS = [5_000, 30_000]

/**
 * RETRY_DELAYS_MS: comma-separated milliseconds, one entry per retry tier
 * (e.g. "5000,30000"). Max attempts = tiers + 1. Both bus adapters read it through this
 * one function so they can never disagree on the retry chain. Invalid values fail at boot
 * rather than silently falling back — a wrong retry schedule is a config bug, not a default.
 */
export function retryDelaysFromConfig(config: ConfigService): number[] {
  const raw = config.get<string>('RETRY_DELAYS_MS')
  if (!raw) return DEFAULT_RETRY_DELAYS_MS

  const delays = raw.split(',').map((part) => Number(part.trim()))
  if (delays.length === 0 || delays.some((ms) => !Number.isInteger(ms) || ms <= 0)) {
    throw new Error(`RETRY_DELAYS_MS must be comma-separated positive integers, got "${raw}"`)
  }
  return delays
}
