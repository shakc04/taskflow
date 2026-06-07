import { Scheduler } from '../src/core/scheduler';
import { WorkerPool } from '../src/core/worker-pool';
import { JobQueue } from '../src/core/queue';
import { JobStore } from '../src/store/sqlite';
import { Job } from '../src/core/job';

const flush = (): Promise<void> => new Promise<void>((resolve) => setImmediate(resolve));

function makeDeferredJob(id: string, runAt: number): Job {
  return {
    id,
    type: 'test',
    payload: {},
    priority: 1,
    status: 'pending',
    created_at: runAt - 1,
    started_at: null,
    finished_at: null,
    error: null,
    run_at: runAt,
  };
}

describe('Scheduler', () => {
  let store: JobStore;
  let queue: JobQueue;

  beforeEach(() => {
    store = new JobStore(':memory:');
    queue = new JobQueue();
  });

  afterEach(() => {
    store.close();
    jest.restoreAllMocks();
  });

  it('does not load a future-dated job before its run_at', () => {
    const base = 1_000_000;
    store.insert(makeDeferredJob('later', base + 5000));
    const pool = new WorkerPool(queue, store, 1, () => Promise.resolve());
    const scheduler = new Scheduler(store, pool, 1000);

    scheduler.pollOnce(base);

    expect(queue.size).toBe(0);
  });

  it('loads a due job into the queue once it has come due', () => {
    const base = 1_000_000;
    store.insert(makeDeferredJob('soon', base + 5000));
    const pool = new WorkerPool(queue, store, 1, () => Promise.resolve());
    const scheduler = new Scheduler(store, pool, 1000);

    scheduler.pollOnce(base);
    expect(queue.size).toBe(0);

    scheduler.pollOnce(base + 6000);
    expect(queue.size).toBe(1);
  });

  it('runs a deferred job only after a simulated clock advance', async () => {
    const base = 1_000_000;
    jest.spyOn(Date, 'now').mockReturnValue(base);
    store.insert(makeDeferredJob('clocked', base + 5000));
    const handler = jest.fn<Promise<void>, [Job]>().mockResolvedValue(undefined);
    const pool = new WorkerPool(queue, store, 1, handler);
    const scheduler = new Scheduler(store, pool, 1000);

    pool.start();
    scheduler.pollOnce();
    expect(handler).not.toHaveBeenCalled();

    (Date.now as jest.Mock).mockReturnValue(base + 6000);
    scheduler.pollOnce();
    await flush();

    expect(handler).toHaveBeenCalledTimes(1);
    await pool.stop();
  });

  it('does not hand the pool the same pending job twice across polls', () => {
    const base = 1_000_000;
    store.insert(makeDeferredJob('once', base));
    const pool = new WorkerPool(queue, store, 1, () => Promise.resolve());
    const scheduler = new Scheduler(store, pool, 1000);

    scheduler.pollOnce(base);
    scheduler.pollOnce(base);
    scheduler.pollOnce(base);

    expect(queue.size).toBe(1);
  });
});
