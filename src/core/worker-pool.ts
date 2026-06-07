import { Job } from './job';
import { JobQueue } from './queue';
import { JobStore } from '../store/sqlite';

export type JobHandler = (job: Job) => Promise<void>;

/**
 * Pulls jobs off the queue and runs the handler on up to `concurrency` of them
 * at once. Status transitions are written back to the store as they happen, so
 * a restart can reload whatever was still pending.
 */
export class WorkerPool {
  private running = false;
  private activeWorkers = 0;
  private readonly inflight = new Set<Promise<void>>();

  constructor(
    private readonly queue: JobQueue,
    private readonly store: JobStore,
    private readonly concurrency: number,
    private readonly handler: JobHandler,
  ) {
    if (concurrency < 1) {
      throw new Error('concurrency must be at least 1');
    }
  }

  /**
   * Loads pending jobs left over from a previous run and begins draining. Jobs
   * are reloaded in priority order by the queue itself, not by insertion order.
   */
  start(): void {
    if (this.running) {
      return;
    }
    this.running = true;
    for (const job of this.store.loadPending()) {
      this.queue.enqueue(job);
    }
    this.drainQueue();
  }

  /**
   * Signals that a job was added to the queue from outside the pool, for
   * instance by the HTTP layer, so the pool should try to pick it up now rather
   * than waiting for a running job to finish.
   */
  notify(): void {
    if (this.running) {
      this.drainQueue();
    }
  }

  /**
   * Stops picking up new jobs and resolves once the jobs already in flight have
   * settled. Jobs still pending in the queue are left for the next start.
   */
  async stop(): Promise<void> {
    this.running = false;
    await Promise.allSettled(this.inflight);
  }

  private drainQueue(): void {
    // Capacity is checked before dequeue on purpose. Pulling a job we cannot
    // run yet would force us to push it back, and that reorders nothing but
    // still churns the heap for no reason.
    while (this.running && this.activeWorkers < this.concurrency && this.queue.size > 0) {
      const job = this.queue.dequeue();
      if (job === null) {
        break;
      }
      // The heap has no remove, so a job cancelled through the API is still
      // sitting here. Confirm it is still pending in the store before running,
      // and otherwise drop it.
      const current = this.store.getById(job.id);
      if (current === null || current.status !== 'pending') {
        continue;
      }
      this.runJob(current);
    }
  }

  private runJob(job: Job): void {
    this.activeWorkers += 1;
    job.status = 'running';
    job.started_at = Date.now();
    this.store.update(job);

    const task = this.handler(job)
      .then(() => {
        job.status = 'done';
        job.finished_at = Date.now();
        job.error = null;
        this.store.update(job);
      })
      .catch((cause: unknown) => {
        job.status = 'failed';
        job.finished_at = Date.now();
        job.error = cause instanceof Error ? cause.message : String(cause);
        this.store.update(job);
      })
      .finally(() => {
        this.activeWorkers -= 1;
        this.inflight.delete(task);
        // A slot just freed up, so try to pull the next job.
        this.drainQueue();
      });

    this.inflight.add(task);
  }
}
