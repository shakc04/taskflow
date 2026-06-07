import { WorkerPool } from '../src/core/worker-pool';
import { JobQueue } from '../src/core/queue';
import { JobStore } from '../src/store/sqlite';
import { Job } from '../src/core/job';

const flush = (): Promise<void> => new Promise<void>((resolve) => setImmediate(resolve));

function makeJob(id: string): Job {
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
  };
}

describe('WorkerPool hooks', () => {
  let store: JobStore;
  let queue: JobQueue;

  beforeEach(() => {
    store = new JobStore(':memory:');
    queue = new JobQueue();
  });

  afterEach(() => {
    store.close();
  });

  it('fires enqueue, start, and complete hooks with the matching job state', async () => {
    store.insert(makeJob('a'));
    const statusAt: Record<string, string> = {};
    const hooks = {
      onEnqueue: jest.fn((job: Job) => {
        statusAt.enqueue = job.status;
      }),
      onStart: jest.fn((job: Job) => {
        statusAt.start = job.status;
      }),
      onComplete: jest.fn((job: Job) => {
        statusAt.complete = job.status;
      }),
      onFail: jest.fn(),
    };
    const pool = new WorkerPool(queue, store, 1, () => Promise.resolve(), { hooks });

    pool.start();
    await pool.stop();
    await flush();

    expect(statusAt.enqueue).toBe('pending');
    expect(statusAt.start).toBe('running');
    expect(statusAt.complete).toBe('done');
    expect(hooks.onFail).not.toHaveBeenCalled();
  });

  it('fires the fail hook with the failed job when the handler rejects', async () => {
    store.insert(makeJob('b'));
    let failedError: string | null = null;
    let failedStatus: string | null = null;
    const hooks = {
      onComplete: jest.fn(),
      onFail: jest.fn((job: Job) => {
        failedError = job.error;
        failedStatus = job.status;
      }),
    };
    const pool = new WorkerPool(queue, store, 1, () => Promise.reject(new Error('it broke')), {
      hooks,
    });

    pool.start();
    await pool.stop();
    await flush();

    expect(failedStatus).toBe('failed');
    expect(failedError).toBe('it broke');
    expect(hooks.onComplete).not.toHaveBeenCalled();
  });

  it('fires hooks in enqueue then start then complete order', async () => {
    store.insert(makeJob('c'));
    const order: string[] = [];
    const hooks = {
      onEnqueue: jest.fn(() => {
        order.push('enqueue');
      }),
      onStart: jest.fn(() => {
        order.push('start');
      }),
      onComplete: jest.fn(() => {
        order.push('complete');
      }),
    };
    const pool = new WorkerPool(queue, store, 1, () => Promise.resolve(), { hooks });

    pool.start();
    await pool.stop();
    await flush();

    expect(order).toEqual(['enqueue', 'start', 'complete']);
  });

  it('completes the job even when a hook throws synchronously', async () => {
    store.insert(makeJob('d'));
    const hooks = {
      onComplete: jest.fn(() => {
        throw new Error('hook blew up');
      }),
    };
    const pool = new WorkerPool(queue, store, 1, () => Promise.resolve(), { hooks });

    pool.start();
    await pool.stop();
    await flush();

    expect(hooks.onComplete).toHaveBeenCalledTimes(1);
    expect(store.getById('d')?.status).toBe('done');
  });

  it('completes the job even when a hook returns a rejected promise', async () => {
    store.insert(makeJob('e'));
    const hooks = {
      onStart: jest.fn(() => Promise.reject(new Error('async hook failure'))),
    };
    const pool = new WorkerPool(queue, store, 1, () => Promise.resolve(), { hooks });

    pool.start();
    await pool.stop();
    await flush();

    expect(hooks.onStart).toHaveBeenCalledTimes(1);
    expect(store.getById('e')?.status).toBe('done');
  });
});
