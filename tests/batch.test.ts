import request from 'supertest';
import { Express } from 'express';
import { JobQueue } from '../src/core/queue';
import { JobStore } from '../src/store/sqlite';
import { WorkerPool } from '../src/core/worker-pool';
import { createServer } from '../src/api/server';

describe('POST /jobs/batch', () => {
  let store: JobStore;
  let app: Express;

  beforeEach(() => {
    store = new JobStore(':memory:');
    const queue = new JobQueue();
    const pool = new WorkerPool(queue, store, 1, () => Promise.resolve());
    app = createServer(queue, store, pool);
  });

  afterEach(() => {
    store.close();
  });

  it('creates every job in a valid batch and returns their ids', async () => {
    const response = await request(app)
      .post('/jobs/batch')
      .send([
        { type: 'a', payload: {}, priority: 1 },
        { type: 'b', payload: { n: 2 }, priority: 5 },
        { type: 'c', payload: {}, priority: 3 },
      ]);

    expect(response.status).toBe(201);
    expect(Array.isArray(response.body)).toBe(true);
    expect(response.body).toHaveLength(3);

    const all = await request(app).get('/jobs');
    expect(all.body).toHaveLength(3);
  });

  it('rejects the whole batch and creates nothing when one item is invalid', async () => {
    const response = await request(app)
      .post('/jobs/batch')
      .send([
        { type: 'a', payload: {}, priority: 1 },
        { type: 'b', payload: {} },
        { type: 'c', payload: {}, priority: 3 },
      ]);

    expect(response.status).toBe(400);
    expect(response.text).toContain('job at index 1');
    expect(response.text).toContain('priority must be a finite number');

    const all = await request(app).get('/jobs');
    expect(all.body).toHaveLength(0);
  });

  it('rejects an empty batch', async () => {
    const response = await request(app).post('/jobs/batch').send([]);
    expect(response.status).toBe(400);
    expect(response.text).toBe('batch must contain at least one job');
  });

  it('rejects a batch larger than the limit', async () => {
    const items = Array.from({ length: 101 }, (_unused, index) => ({
      type: 'bulk',
      payload: { index },
      priority: 1,
    }));
    const response = await request(app).post('/jobs/batch').send(items);
    expect(response.status).toBe(400);
    expect(response.text).toBe('batch cannot exceed 100 jobs');
  });

  it('rejects a body that is not an array', async () => {
    const response = await request(app)
      .post('/jobs/batch')
      .send({ type: 'a', payload: {}, priority: 1 });
    expect(response.status).toBe(400);
    expect(response.text).toBe('body must be an array of jobs');
  });
});
