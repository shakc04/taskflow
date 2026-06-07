import { WorkerPool } from '../src/core/worker-pool';
import { JobQueue } from '../src/core/queue';
import { JobStore } from '../src/store/sqlite';
import { Job } from '../src/core/job';

function makeJob(id: string, priority: number): Job {
  return {
    id,
    type: 'test',
    payload: {},
    priority,
    status: 'pending',
    created_at: Date.now(),
    started_at: null,
    finished_at: null,
    error: null,
  };
}

// Lets the chained .then/.catch/.finally on a job settle before assertions.
const flush = (): Promise<void> => new Promise<void>((resolve) => setImmediate(resolve));

describe('WorkerPool', () => {
  let store: JobStore;
  let queue: JobQueue;

  beforeEach(() => {
    store = new JobStore(':memory:');
    queue = new JobQueue();
  });

  afterEach(() => {
    store.close();
  });

  it('runs jobs only up to the concurrency limit', async () => {
    for (const id of ['a', 'b', 'c']) {
      store.insert(makeJob(id, 1));
    }

    const releases: Array<() => void> = [];
    const handler = jest.fn(
      () => new Promise<void>((resolve) => releases.push(resolve)),
    );
    const pool = new WorkerPool(queue, store, 2, handler);

    pool.start();
    expect(handler).toHaveBeenCalledTimes(2);

    // Freeing one slot should let the third job start.
    const release = releases[0];
    if (release !== undefined) {
      release();
    }
    await flush();
    expect(handler).toHaveBeenCalledTimes(3);

    for (const remaining of releases) {
      remaining();
    }
    await pool.stop();
  });

  it('marks a job done when the handler resolves', async () => {
    store.insert(makeJob('ok', 1));
    const handler = jest.fn<Promise<void>, [Job]>().mockResolvedValue(undefined);
    const pool = new WorkerPool(queue, store, 1, handler);

    pool.start();
    await pool.stop();

    const job = store.getById('ok');
    expect(job?.status).toBe('done');
    expect(job?.finished_at).not.toBeNull();
    expect(job?.error).toBeNull();
  });

  it('marks a job failed and records the message when the handler rejects', async () => {
    store.insert(makeJob('bad', 1));
    const handler = jest
      .fn<Promise<void>, [Job]>()
      .mockRejectedValue(new Error('handler exploded'));
    const pool = new WorkerPool(queue, store, 1, handler);

    pool.start();
    await pool.stop();

    const job = store.getById('bad');
    expect(job?.status).toBe('failed');
    expect(job?.error).toBe('handler exploded');
    expect(job?.finished_at).not.toBeNull();
  });

  it('skips a job that was cancelled from the store before it ran', async () => {
    store.insert(makeJob('first', 1));
    store.insert(makeJob('cancelled', 2));
    const handler = jest.fn<Promise<void>, [Job]>().mockResolvedValue(undefined);
    const pool = new WorkerPool(queue, store, 1, handler);

    // Cancel the lower priority job before any slot frees up for it.
    store.delete('cancelled');

    pool.start();
    await pool.stop();

    expect(handler).toHaveBeenCalledTimes(1);
    expect(handler.mock.calls[0]?.[0]?.id).toBe('first');
  });
});
