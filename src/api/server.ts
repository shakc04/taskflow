import express, { Express } from 'express';
import { JobQueue } from '../core/queue';
import { JobStore } from '../store/sqlite';
import { WorkerPool } from '../core/worker-pool';
import { createRouter } from './routes';

export function createServer(queue: JobQueue, store: JobStore, pool: WorkerPool): Express {
  const app = express();
  app.use(express.json());
  app.use(createRouter(queue, store, pool));
  return app;
}
