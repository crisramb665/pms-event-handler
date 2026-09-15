export interface ConfirmationEmailRequest {
  reservationId: string
  guestEmail: string
  guestName: string
}

export interface EmailSender {
  sendConfirmation(request: ConfirmationEmailRequest): Promise<void>
}

/**
 * Simulator, not a real transport. Failures thrown here are transient by design (§5:
 * "simulated email sender down" is the example transient failure) — the caller is expected
 * to retry through the normal retry chain, capped at 3 attempts, same as any other handler
 * failure. It must never throw NonRetryableError: that would skip retries entirely and
 * defeat the point of this class, which is to exercise the retry path (feed scenario 8).
 */
export class SimulatedEmailSender implements EmailSender {
  private failuresRemaining = 0

  /** Test/demo affordance: makes the next N sends fail before succeeding again. */
  failNextN(n: number): void {
    this.failuresRemaining = n
  }

  async sendConfirmation(request: ConfirmationEmailRequest): Promise<void> {
    if (this.failuresRemaining > 0) {
      this.failuresRemaining -= 1
      throw new Error(`simulated email send failure for reservation ${request.reservationId}`)
    }
  }
}
