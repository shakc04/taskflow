import request from 'supertest';
import { Express } from 'express';
import { JobQueue } from '../src/core/queue';
import { JobStore } from '../src/store/sqlite';
import { DeadLetterStore } from '../src/store/dlq';
import { WorkerPool } from '../src/core/worker-pool';
import { createServer } from '../src/api/server';
import { Job } from '../src/core/job';
import { RetryPolicy } from '../src/core/retry';

const flush = (): Promise<void> => new Promise<void>((resolve) => setImmediate(resolve));

const samplePolicy: RetryPolicy = {
  max_attempts: 2,
  base_delay_ms: 50,
  max_delay_ms: 1000,
  jitter: false,
};

function makeFailedJob(id: string): Job {
  return {
    id,
    type: 'test',
    payload: { value: id },
    priority: 1,
    status: 'failed',
    created_at: Date.now(),
    started_at: Date.now(),
    finished_at: Date.now(),
    error: 'boom',
    run_at: Date.now(),
    retry_policy: samplePolicy,
    attempts_remaining: 0,
  };
}

describe('DeadLetterStore', () => {
  let store: JobStore;
  let dlq: DeadLetterStore;

  beforeEach(() => {
    store = new JobStore(':memory:');
    dlq = new DeadLetterStore(store.getConnection());
  });

  afterEach(() => {
    store.close();
  });

  it('stores an entry with its failure reason and moved_at timestamp', () => {
    dlq.add(makeFailedJob('x'), 'handler gave up', 1700);

    const entry = dlq.getById('x');
    expect(entry?.failure_reason).toBe('handler gave up');
    expect(entry?.moved_at).toBe(1700);
    expect(entry?.payload).toEqual({ value: 'x' });
    expect(entry?.retry_policy).toEqual(samplePolicy);
  });

  it('lists entries and removes them by id', () => {
    dlq.add(makeFailedJob('one'), 'reason one', 1);
    dlq.add(makeFailedJob('two'), 'reason two', 2);
    expect(dlq.list()).toHaveLength(2);

    dlq.remove('one');
    expect(dlq.getById('one')).toBeNull();
    expect(dlq.list()).toHaveLength(1);
  });
});

describe('DLQ HTTP routes', () => {
  let store: JobStore;
  let dlq: DeadLetterStore;
  let app: Express;

  beforeEach(() => {
    store = new JobStore(':memory:');
    dlq = new DeadLetterStore(store.getConnection());
    const queue = new JobQueue();
    const pool = new WorkerPool(queue, store, 1, () => Promise.resolve(), { dlq });
    app = createServer(queue, store, pool, dlq);
  });

  afterEach(() => {
    store.close();
  });

  it('moves a job into the dlq after its attempts are exhausted and lists it', async () => {
    const job: Job = {
      ...makeFailedJob('exhaust'),
      status: 'pending',
      attempts_remaining: 1,
    };
    store.insert(job);
    const failingPool = new WorkerPool(new JobQueue(), store, 1, () => Promise.reject(new Error('always fails')), { dlq });

    failingPool.start();
    await failingPool.stop();
    await flush();

    const listed = await request(app).get('/dlq');
    expect(listed.status).toBe(200);
    expect(listed.body).toHaveLength(1);
    expect(listed.body[0].id).toBe('exhaust');
    expect(listed.body[0].failure_reason).toBe('always fails');
  });

  it('returns a single dlq entry and 404 for an unknown id', async () => {
    dlq.add(makeFailedJob('known'), 'because', 5);

    const found = await request(app).get('/dlq/known');
    expect(found.status).toBe(200);
    expect(found.body.id).toBe('known');

    const missing = await request(app).get('/dlq/missing');
    expect(missing.status).toBe(404);
  });

  it('requeues a dlq entry back to pending and clears it from the dlq', async () => {
    dlq.add(makeFailedJob('comeback'), 'transient outage', 9);

    const requeued = await request(app).post('/dlq/comeback/requeue');
    expect(requeued.status).toBe(200);
    expect(requeued.body.status).toBe('pending');
    expect(requeued.body.attempts_remaining).toBe(samplePolicy.max_attempts);

    expect(dlq.getById('comeback')).toBeNull();
    const back = await request(app).get('/jobs/comeback');
    expect(back.status).toBe(200);
    expect(back.body.status).toBe('pending');
  });
});
