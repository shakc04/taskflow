import express, { Express } from 'express';
import { JobQueue } from '../core/queue';
import { JobStore } from '../store/sqlite';
import { DeadLetterStore } from '../store/dlq';
import { WorkerPool } from '../core/worker-pool';
import { createRouter } from './routes';

export function createServer(
  queue: JobQueue,
  store: JobStore,
  pool: WorkerPool,
  dlq?: DeadLetterStore,
): Express {
  const app = express();
  app.use(express.json());
  app.use(createRouter(queue, store, pool, dlq));
  return app;
}
