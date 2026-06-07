import { Router, Request, Response } from 'express';
import { randomUUID } from 'node:crypto';
import { Job, JobStatus } from '../core/job';
import { RetryPolicy } from '../core/retry';
import { JobQueue } from '../core/queue';
import { JobStore } from '../store/sqlite';
import { DeadLetterStore } from '../store/dlq';
import { WorkerPool } from '../core/worker-pool';

const VALID_STATUSES: readonly JobStatus[] = ['pending', 'running', 'done', 'failed'];
const MAX_BATCH_SIZE = 100;

interface EnqueueBody {
  type: string;
  payload: Record<string, unknown>;
  priority: number;
  run_at?: number;
  retry_policy?: RetryPolicy;
}

function parseRetryPolicy(value: unknown): RetryPolicy | { error: string } {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return { error: 'retry_policy must be an object' };
  }
  const candidate = value as Record<string, unknown>;
  if (
    typeof candidate.max_attempts !== 'number' ||
    !Number.isInteger(candidate.max_attempts) ||
    candidate.max_attempts < 1
  ) {
    return { error: 'retry_policy.max_attempts must be a positive integer' };
  }
  if (typeof candidate.base_delay_ms !== 'number' || !Number.isFinite(candidate.base_delay_ms) || candidate.base_delay_ms < 0) {
    return { error: 'retry_policy.base_delay_ms must be a non-negative number' };
  }
  if (typeof candidate.max_delay_ms !== 'number' || !Number.isFinite(candidate.max_delay_ms) || candidate.max_delay_ms < 0) {
    return { error: 'retry_policy.max_delay_ms must be a non-negative number' };
  }
  if (typeof candidate.jitter !== 'boolean') {
    return { error: 'retry_policy.jitter must be a boolean' };
  }
  return {
    max_attempts: candidate.max_attempts,
    base_delay_ms: candidate.base_delay_ms,
    max_delay_ms: candidate.max_delay_ms,
    jitter: candidate.jitter,
  };
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

  const parsed: EnqueueBody = {
    type: candidate.type,
    priority: candidate.priority,
    payload: payload as Record<string, unknown>,
  };

  if (candidate.run_at !== undefined) {
    if (typeof candidate.run_at !== 'number' || !Number.isFinite(candidate.run_at)) {
      return { error: 'run_at must be a finite number' };
    }
    parsed.run_at = candidate.run_at;
  }

  if (candidate.retry_policy !== undefined && candidate.retry_policy !== null) {
    const policy = parseRetryPolicy(candidate.retry_policy);
    if ('error' in policy) {
      return { error: policy.error };
    }
    parsed.retry_policy = policy;
  }

  return parsed;
}

function buildJob(parsed: EnqueueBody): Job {
  const now = Date.now();
  const job: Job = {
    id: randomUUID(),
    type: parsed.type,
    payload: parsed.payload,
    priority: parsed.priority,
    status: 'pending',
    created_at: now,
    started_at: null,
    finished_at: null,
    error: null,
    run_at: parsed.run_at ?? now,
    retry_policy: parsed.retry_policy ?? null,
  };
  if (parsed.retry_policy !== undefined) {
    job.attempts_remaining = parsed.retry_policy.max_attempts;
  }
  return job;
}

function isDue(job: Job, now: number): boolean {
  return job.run_at === undefined || job.run_at <= now;
}

export function createRouter(
  queue: JobQueue,
  store: JobStore,
  pool: WorkerPool,
  dlq?: DeadLetterStore,
): Router {
  const router = Router();

  router.post('/jobs', (req: Request, res: Response) => {
    const parsed = parseEnqueueBody(req.body);
    if ('error' in parsed) {
      res.status(400).type('text/plain').send(parsed.error);
      return;
    }

    const job = buildJob(parsed);
    store.insert(job);
    // A future-dated job stays in the store until the scheduler finds it due, so
    // only an immediately runnable job is handed to the pool here.
    if (isDue(job, Date.now())) {
      pool.enqueue(job);
    }

    res.status(201).json(job);
  });

  router.post('/jobs/batch', (req: Request, res: Response) => {
    const body: unknown = req.body;
    if (!Array.isArray(body)) {
      res.status(400).type('text/plain').send('body must be an array of jobs');
      return;
    }
    if (body.length === 0) {
      res.status(400).type('text/plain').send('batch must contain at least one job');
      return;
    }
    if (body.length > MAX_BATCH_SIZE) {
      res.status(400).type('text/plain').send(`batch cannot exceed ${MAX_BATCH_SIZE} jobs`);
      return;
    }

    const jobs: Job[] = [];
    for (let index = 0; index < body.length; index += 1) {
      const parsed = parseEnqueueBody(body[index]);
      if ('error' in parsed) {
        res
          .status(400)
          .type('text/plain')
          .send(`job at index ${index} is invalid: ${parsed.error}`);
        return;
      }
      jobs.push(buildJob(parsed));
    }

    // All or nothing. The whole batch is validated before a single row is
    // written so that one bad item never leaves the caller with a partial set
    // of jobs to reconcile.
    const now = Date.now();
    const ids: string[] = [];
    for (const job of jobs) {
      store.insert(job);
      if (isDue(job, now)) {
        pool.enqueue(job);
      }
      ids.push(job.id);
    }

    res.status(201).json(ids);
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

  if (dlq !== undefined) {
    const deadLetters = dlq;

    router.get('/dlq', (_req: Request, res: Response) => {
      res.json(deadLetters.list());
    });

    router.get('/dlq/:id', (req: Request, res: Response) => {
      const id = req.params.id;
      if (id === undefined) {
        res.status(400).type('text/plain').send('id is required');
        return;
      }
      const entry = deadLetters.getById(id);
      if (entry === null) {
        res.status(404).type('text/plain').send('dlq entry not found');
        return;
      }
      res.json(entry);
    });

    router.post('/dlq/:id/requeue', (req: Request, res: Response) => {
      const id = req.params.id;
      if (id === undefined) {
        res.status(400).type('text/plain').send('id is required');
        return;
      }
      const entry = deadLetters.getById(id);
      if (entry === null) {
        res.status(404).type('text/plain').send('dlq entry not found');
        return;
      }

      const now = Date.now();
      const policy = entry.retry_policy ?? null;
      const job: Job = {
        id: entry.id,
        type: entry.type,
        payload: entry.payload,
        priority: entry.priority,
        status: 'pending',
        created_at: entry.created_at,
        started_at: null,
        finished_at: null,
        error: null,
        run_at: now,
        retry_policy: policy,
      };
      // The job comes back with its attempt budget reset so a requeue is a
      // genuine fresh start rather than a single last try.
      if (policy !== null) {
        job.attempts_remaining = policy.max_attempts;
      }

      store.insert(job);
      deadLetters.remove(entry.id);
      pool.enqueue(job);

      res.status(200).json(job);
    });
  }

  return router;
}
