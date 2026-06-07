export interface RetryPolicy {
  max_attempts: number;
  base_delay_ms: number;
  max_delay_ms: number;
  jitter: boolean;
}

/**
 * Exponential backoff for retry number `attempt`, counted from zero, so the
 * first retry waits base_delay_ms, the second waits twice that, and so on, with
 * the result clamped to max_delay_ms.
 */
export function computeDelay(attempt: number, policy: RetryPolicy): number {
  const exponential = policy.base_delay_ms * Math.pow(2, attempt);
  const capped = Math.min(exponential, policy.max_delay_ms);
  if (!policy.jitter) {
    return capped;
  }
  // Jitter pulls each delay down by a random fraction so that a batch of jobs
  // which failed at the same instant does not wake up and retry in lockstep,
  // which would stampede whatever downstream they share.
  const factor = 0.5 + Math.random() * 0.5;
  return capped * factor;
}
