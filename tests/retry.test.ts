import { computeDelay, RetryPolicy } from '../src/core/retry';
import { WorkerPool } from '../src/core/worker-pool';
import { JobQueue } from '../src/core/queue';
import { JobStore } from '../src/store/sqlite';
import { DeadLetterStore } from '../src/store/dlq';
import { Job } from '../src/core/job';

const flush = (): Promise<void> => new Promise<void>((resolve) => setImmediate(resolve));

function policy(overrides: Partial<RetryPolicy> = {}): RetryPolicy {
  return {
    max_attempts: 3,
    base_delay_ms: 100,
    max_delay_ms: 10_000,
    jitter: false,
    ...overrides,
  };
}

function makeRetryJob(id: string, retry: RetryPolicy): Job {
  return {
    id,
    type: 'test',
    payload: {},
    priority: 1,
    status: 'pending',
    created_at: Date.now(),
    started_at: null,
    finished_at: null,
    error: null,
    run_at: Date.now(),
    retry_policy: retry,
    attempts_remaining: retry.max_attempts,
  };
}

describe('computeDelay', () => {
  it('returns the base delay at attempt zero', () => {
    expect(computeDelay(0, policy())).toBe(100);
  });

  it('doubles the delay at each exponent step', () => {
    const used = policy();
    expect(computeDelay(1, used)).toBe(200);
    expect(computeDelay(2, used)).toBe(400);
    expect(computeDelay(3, used)).toBe(800);
  });

  it('clamps the delay to max_delay_ms', () => {
    const used = policy({ base_delay_ms: 1000, max_delay_ms: 3000 });
    expect(computeDelay(0, used)).toBe(1000);
    expect(computeDelay(2, used)).toBe(3000);
    expect(computeDelay(10, used)).toBe(3000);
  });

  it('keeps every jittered sample within half the capped delay and the cap', () => {
    const used = policy({ base_delay_ms: 500, jitter: true });
    const capped = 500;
    for (let sample = 0; sample < 1000; sample += 1) {
      const delay = computeDelay(0, used);
      expect(delay).toBeGreaterThanOrEqual(capped * 0.5);
      expect(delay).toBeLessThanOrEqual(capped);
    }
  });

  it('applies jitter relative to the capped value, not the raw exponential', () => {
    const used = policy({ base_delay_ms: 4000, max_delay_ms: 5000, jitter: true });
    for (let sample = 0; sample < 1000; sample += 1) {
      const delay = computeDelay(8, used);
      expect(delay).toBeGreaterThanOrEqual(5000 * 0.5);
      expect(delay).toBeLessThanOrEqual(5000);
    }
  });
});

describe('WorkerPool retry behavior', () => {
  let store: JobStore;
  let queue: JobQueue;

  beforeEach(() => {
    store = new JobStore(':memory:');
    queue = new JobQueue();
  });

  afterEach(() => {
    store.close();
  });

  it('re-schedules a failed job with a decremented attempt count and a future run_at', async () => {
    const job = makeRetryJob('retry-me', policy({ max_attempts: 3 }));
    store.insert(job);
    const handler = jest.fn<Promise<void>, [Job]>().mockRejectedValue(new Error('first failure'));
    const pool = new WorkerPool(queue, store, 1, handler);

    pool.start();
    await pool.stop();
    await flush();

    const stored = store.getById('retry-me');
    expect(stored?.status).toBe('pending');
    expect(stored?.attempts_remaining).toBe(2);
    expect(stored?.error).toBe('first failure');
    expect(stored?.started_at).toBeNull();
    expect((stored?.run_at ?? 0)).toBeGreaterThan(stored?.created_at ?? 0);
    expect(handler).toHaveBeenCalledTimes(1);
  });

  it('moves a job to the dead letter store once its final attempt is exhausted', async () => {
    const dlq = new DeadLetterStore(store.getConnection());
    const job = makeRetryJob('done-trying', policy({ max_attempts: 1 }));
    store.insert(job);
    const handler = jest.fn<Promise<void>, [Job]>().mockRejectedValue(new Error('terminal failure'));
    const pool = new WorkerPool(queue, store, 1, handler, { dlq });

    pool.start();
    await pool.stop();
    await flush();

    expect(store.getById('done-trying')).toBeNull();
    const dead = dlq.getById('done-trying');
    expect(dead?.failure_reason).toBe('terminal failure');
    expect(dead?.status).toBe('failed');
  });

  it('marks an exhausted job failed in place when no dead letter store is configured', async () => {
    const job = makeRetryJob('no-dlq', policy({ max_attempts: 1 }));
    store.insert(job);
    const handler = jest.fn<Promise<void>, [Job]>().mockRejectedValue(new Error('nowhere to go'));
    const pool = new WorkerPool(queue, store, 1, handler);

    pool.start();
    await pool.stop();
    await flush();

    const stored = store.getById('no-dlq');
    expect(stored?.status).toBe('failed');
    expect(stored?.error).toBe('nowhere to go');
  });

  it('does not retry a job whose handler resolves', async () => {
    const dlq = new DeadLetterStore(store.getConnection());
    const job = makeRetryJob('happy', policy({ max_attempts: 3 }));
    store.insert(job);
    const handler = jest.fn<Promise<void>, [Job]>().mockResolvedValue(undefined);
    const pool = new WorkerPool(queue, store, 1, handler, { dlq });

    pool.start();
    await pool.stop();
    await flush();

    expect(store.getById('happy')?.status).toBe('done');
    expect(dlq.getById('happy')).toBeNull();
    expect(handler).toHaveBeenCalledTimes(1);
  });
});
