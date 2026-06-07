import { Router, Request, Response } from 'express';
import { randomUUID } from 'node:crypto';
import { Job, JobStatus } from '../core/job';
import { JobQueue } from '../core/queue';
import { JobStore } from '../store/sqlite';
import { WorkerPool } from '../core/worker-pool';

const VALID_STATUSES: readonly JobStatus[] = ['pending', 'running', 'done', 'failed'];

interface EnqueueBody {
  type: string;
  payload: Record<string, unknown>;
  priority: number;
}

/**
 * Validates an untyped request body into the fields needed to build a job.
 * Returns either the parsed body or a message describing the first problem,
 * which the route turns into a 400.
 */
function parseEnqueueBody(body: unknown): EnqueueBody | { error: string } {
  if (typeof body !== 'object' || body === null) {
    return { error: 'body must be a json object' };
  }
  const candidate = body as Record<string, unknown>;

  if (typeof candidate.type !== 'string' || candidate.type.length === 0) {
    return { error: 'type must be a non-empty string' };
  }
  if (typeof candidate.priority !== 'number' || !Number.isFinite(candidate.priority)) {
    return { error: 'priority must be a finite number' };
  }
  const payload = candidate.payload;
  if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) {
    return { error: 'payload must be an object' };
  }

  return {
    type: candidate.type,
    priority: candidate.priority,
    payload: payload as Record<string, unknown>,
  };
}

export function createRouter(queue: JobQueue, store: JobStore, pool: WorkerPool): Router {
  const router = Router();

  router.post('/jobs', (req: Request, res: Response) => {
    const parsed = parseEnqueueBody(req.body);
    if ('error' in parsed) {
      res.status(400).type('text/plain').send(parsed.error);
      return;
    }

    const job: Job = {
      id: randomUUID(),
      type: parsed.type,
      payload: parsed.payload,
      priority: parsed.priority,
      status: 'pending',
      created_at: Date.now(),
      started_at: null,
      finished_at: null,
      error: null,
    };

    store.insert(job);
    queue.enqueue(job);
    pool.notify();

    res.status(201).json(job);
  });

  router.get('/jobs', (req: Request, res: Response) => {
    const status = req.query.status;
    if (status === undefined) {
      res.json(store.list());
      return;
    }
    if (typeof status !== 'string' || !VALID_STATUSES.includes(status as JobStatus)) {
      res.status(400).type('text/plain').send('status must be one of pending, running, done, failed');
      return;
    }
    res.json(store.list(status as JobStatus));
  });

  router.get('/jobs/:id', (req: Request, res: Response) => {
    const id = req.params.id;
    if (id === undefined) {
      res.status(400).type('text/plain').send('id is required');
      return;
    }
    const job = store.getById(id);
    if (job === null) {
      res.status(404).type('text/plain').send('job not found');
      return;
    }
    res.json(job);
  });

  router.delete('/jobs/:id', (req: Request, res: Response) => {
    const id = req.params.id;
    if (id === undefined) {
      res.status(400).type('text/plain').send('id is required');
      return;
    }
    const job = store.getById(id);
    if (job === null) {
      res.status(404).type('text/plain').send('job not found');
      return;
    }
    if (job.status !== 'pending') {
      res.status(409).type('text/plain').send(`cannot cancel a job that is ${job.status}`);
      return;
    }
    store.delete(id);
    res.status(204).send();
  });

  return router;
}
