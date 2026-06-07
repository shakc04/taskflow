import { RetryPolicy } from './retry';

export type JobStatus = 'pending' | 'running' | 'done' | 'failed';

export interface Job {
  id: string;
  type: string;
  payload: Record<string, unknown>;
  /** Lower number wins. The caller decides what the scale means for their domain. */
  priority: number;
  status: JobStatus;
  created_at: number;
  started_at: number | null;
  finished_at: number | null;
  error: string | null;
  /** Earliest time the job may run, Unix ms. Absent means runnable immediately. */
  run_at?: number;
  /** Null or absent means the job is not retried when its handler throws. */
  retry_policy?: RetryPolicy | null;
  /** Attempts still permitted, including the current one. Set when a policy is present. */
  attempts_remaining?: number;
}
