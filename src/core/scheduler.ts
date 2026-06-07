import { JobStore } from '../store/sqlite';
import { WorkerPool } from './worker-pool';

/**
 * Watches the store for deferred jobs whose run_at has arrived and hands them
 * to the worker pool. It polls on a timer rather than relying on SQLite
 * triggers because a trigger fires inside the writing connection and cannot
 * announce that a future run_at has simply passed; only a clock can notice
 * that, so the scheduler re-reads due jobs on each tick.
 */
export class Scheduler {
  private timer: NodeJS.Timeout | null = null;
  private readonly enqueuedIds = new Set<string>();

  constructor(
    private readonly store: JobStore,
    private readonly pool: WorkerPool,
    private readonly intervalMs: number = 1000,
  ) {
    if (intervalMs < 1) {
      throw new Error('scheduler interval must be at least 1 ms');
    }
  }

  start(): void {
    if (this.timer !== null) {
      return;
    }
    this.timer = setInterval(() => this.pollOnce(), this.intervalMs);
    // The timer should not hold the process open on its own when nothing else
    // is running.
    if (typeof this.timer.unref === 'function') {
      this.timer.unref();
    }
    this.pollOnce();
  }

  stop(): void {
    if (this.timer !== null) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  pollOnce(now: number = Date.now()): void {
    const due = this.store.loadDue(now);
    const dueIds = new Set<string>();
    for (const job of due) {
      dueIds.add(job.id);
      // A due job stays pending in the store until a worker actually picks it
      // up, so without this guard the next tick would hand the pool the same
      // job again; the id is tracked until the job is no longer due-pending.
      if (!this.enqueuedIds.has(job.id)) {
        this.enqueuedIds.add(job.id);
        this.pool.enqueue(job);
      }
    }
    for (const id of this.enqueuedIds) {
      if (!dueIds.has(id)) {
        this.enqueuedIds.delete(id);
      }
    }
  }
}
