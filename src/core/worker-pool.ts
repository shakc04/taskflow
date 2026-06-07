import { Job } from './job';
import { JobQueue } from './queue';
import { JobStore } from '../store/sqlite';
import { DeadLetterStore } from '../store/dlq';
import { JobHooks } from './hooks';
import { computeDelay } from './retry';

export type JobHandler = (job: Job) => Promise<void>;

export interface WorkerPoolOptions {
  dlq?: DeadLetterStore;
  hooks?: JobHooks;
}

/**
 * Pulls jobs off the queue and runs the handler on up to `concurrency` of them
 * at once. Status transitions are written back to the store as they happen, so
 * a restart can reload whatever was still pending. A failed job with a retry
 * policy is re-scheduled for a later run, and one that has used all its attempts
 * is moved to the dead letter store when one is configured.
 */
export class WorkerPool {
  private running = false;
  private activeWorkers = 0;
  private readonly inflight = new Set<Promise<void>>();
  private readonly dlq: DeadLetterStore | undefined;
  private readonly hooks: JobHooks;

  constructor(
    private readonly queue: JobQueue,
    private readonly store: JobStore,
    private readonly concurrency: number,
    private readonly handler: JobHandler,
    options: WorkerPoolOptions = {},
  ) {
    if (concurrency < 1) {
      throw new Error('concurrency must be at least 1');
    }
    this.dlq = options.dlq;
    this.hooks = options.hooks ?? {};
  }

  /**
   * Loads due pending jobs left over from a previous run and begins draining.
   * Future-dated jobs are left for the scheduler so the pool does not run them
   * ahead of their run_at.
   */
  start(): void {
    if (this.running) {
      return;
    }
    this.running = true;
    for (const job of this.store.loadDue(Date.now())) {
      this.enqueueJob(job);
    }
    this.drainQueue();
  }

  /**
   * Places a job into the in-memory queue and tries to run it now. The HTTP
   * layer and the scheduler use this to hand the pool a job that is due.
   */
  enqueue(job: Job): void {
    this.enqueueJob(job);
    this.drainQueue();
  }

  /**
   * Signals that a job was added to the queue from outside the pool so the pool
   * should try to pick it up now rather than waiting for a running job to
   * finish.
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

  private enqueueJob(job: Job): void {
    this.queue.enqueue(job);
    this.fireHook(this.hooks.onEnqueue, job);
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
      // The heap has no remove, so a job cancelled through the API, already
      // running from a duplicate enqueue, or not yet due is still sitting here.
      // Confirm it is pending and due in the store before running it.
      const current = this.store.getById(job.id);
      if (current === null || current.status !== 'pending') {
        continue;
      }
      if (current.run_at !== undefined && current.run_at > Date.now()) {
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
    this.fireHook(this.hooks.onStart, job);

    const task = this.handler(job)
      .then(() => {
        job.status = 'done';
        job.finished_at = Date.now();
        job.error = null;
        this.store.update(job);
        this.fireHook(this.hooks.onComplete, job);
      })
      .catch((cause: unknown) => {
        this.handleFailure(job, cause);
      })
      .finally(() => {
        this.activeWorkers -= 1;
        this.inflight.delete(task);
        // A slot just freed up, so try to pull the next job.
        this.drainQueue();
      });

    this.inflight.add(task);
  }

  private handleFailure(job: Job, cause: unknown): void {
    const message = cause instanceof Error ? cause.message : String(cause);
    job.finished_at = Date.now();
    job.error = message;
    job.status = 'failed';
    this.fireHook(this.hooks.onFail, job);

    const policy = job.retry_policy ?? null;
    if (policy !== null) {
      const remaining = job.attempts_remaining ?? policy.max_attempts;
      if (remaining > 1) {
        const retryIndex = policy.max_attempts - remaining;
        job.attempts_remaining = remaining - 1;
        job.status = 'pending';
        job.started_at = null;
        job.run_at = job.finished_at + computeDelay(retryIndex, policy);
        this.store.update(job);
        return;
      }
      if (this.dlq !== undefined) {
        this.dlq.add(job, message, Date.now());
        this.store.delete(job.id);
        return;
      }
    }

    this.store.update(job);
  }

  private fireHook(hook: ((job: Job) => void | Promise<void>) | undefined, job: Job): void {
    if (hook === undefined) {
      return;
    }
    // Hooks are for observation only, so a hook that throws or rejects must not
    // fail the job or surface as an unhandled rejection; the pool calls it
    // without awaiting and discards anything it returns.
    try {
      const result = hook(job);
      if (result instanceof Promise) {
        result.catch(() => {});
      }
    } catch {
      // swallowed for the reason above
    }
  }
}
