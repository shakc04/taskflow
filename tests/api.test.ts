import request from 'supertest';
import { Express } from 'express';
import { JobQueue } from '../src/core/queue';
import { JobStore } from '../src/store/sqlite';
import { WorkerPool } from '../src/core/worker-pool';
import { createServer } from '../src/api/server';

describe('HTTP API', () => {
  let store: JobStore;
  let app: Express;

  beforeEach(() => {
    store = new JobStore(':memory:');
    const queue = new JobQueue();
    // The pool is never started, so enqueued jobs stay pending and the route
    // assertions are not racing a worker that mutates status underneath them.
    const pool = new WorkerPool(queue, store, 1, () => Promise.resolve());
    app = createServer(queue, store, pool);
  });

  afterEach(() => {
    store.close();
  });

  it('POST /jobs creates a pending job', async () => {
    const response = await request(app)
      .post('/jobs')
      .send({ type: 'sleep', payload: { ms: 10 }, priority: 5 });

    expect(response.status).toBe(201);
    expect(response.body.id).toEqual(expect.any(String));
    expect(response.body.status).toBe('pending');
    expect(response.body.priority).toBe(5);
  });

  it('POST /jobs rejects a malformed body with 400', async () => {
    const response = await request(app).post('/jobs').send({ type: 'sleep' });
    expect(response.status).toBe(400);
    expect(response.text).toBe('priority must be a finite number');
  });

  it('GET /jobs lists jobs and filters by status', async () => {
    await request(app).post('/jobs').send({ type: 'a', payload: {}, priority: 1 });
    await request(app).post('/jobs').send({ type: 'b', payload: {}, priority: 2 });

    const all = await request(app).get('/jobs');
    expect(all.status).toBe(200);
    expect(all.body).toHaveLength(2);

    const pending = await request(app).get('/jobs').query({ status: 'pending' });
    expect(pending.body).toHaveLength(2);

    const done = await request(app).get('/jobs').query({ status: 'done' });
    expect(done.body).toHaveLength(0);
  });

  it('GET /jobs/:id returns one job and 404 for an unknown id', async () => {
    const created = await request(app)
      .post('/jobs')
      .send({ type: 'sleep', payload: {}, priority: 1 });
    const id = created.body.id as string;

    const found = await request(app).get(`/jobs/${id}`);
    expect(found.status).toBe(200);
    expect(found.body.id).toBe(id);

    const missing = await request(app).get('/jobs/does-not-exist');
    expect(missing.status).toBe(404);
  });

  it('DELETE /jobs/:id cancels a pending job', async () => {
    const created = await request(app)
      .post('/jobs')
      .send({ type: 'sleep', payload: {}, priority: 1 });
    const id = created.body.id as string;

    const cancelled = await request(app).delete(`/jobs/${id}`);
    expect(cancelled.status).toBe(204);

    const afterwards = await request(app).get(`/jobs/${id}`);
    expect(afterwards.status).toBe(404);
  });
});
