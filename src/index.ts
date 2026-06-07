import { JobQueue } from './core/queue';
import { Job } from './core/job';
import { JobStore } from './store/sqlite';
import { DeadLetterStore } from './store/dlq';
import { WorkerPool } from './core/worker-pool';
import { Scheduler } from './core/scheduler';
import { createServer } from './api/server';

const PORT = Number(process.env.PORT ?? 3000);
const CONCURRENCY = Number(process.env.CONCURRENCY ?? 4);
const DB_FILE = process.env.DB_FILE ?? 'taskflow.sqlite';
const POLL_INTERVAL_MS = Number(process.env.POLL_INTERVAL_MS ?? 1000);

/**
 * Demonstration handler. A real deployment swaps this for domain work keyed on
 * job.type. "sleep" waits for payload.ms before resolving, and any other type
 * fails the job so unknown work does not silently look successful.
 */
async function handle(job: Job): Promise<void> {
  if (job.type === 'sleep') {
    const ms = typeof job.payload.ms === 'number' ? job.payload.ms : 0;
    await new Promise<void>((resolve) => setTimeout(resolve, ms));
    return;
  }
  throw new Error(`no handler registered for type ${job.type}`);
}

function main(): void {
  const store = new JobStore(DB_FILE);
  const dlq = new DeadLetterStore(store.getConnection());
  const queue = new JobQueue();
  const pool = new WorkerPool(queue, store, CONCURRENCY, handle, { dlq });
  const scheduler = new Scheduler(store, pool, POLL_INTERVAL_MS);
  const app = createServer(queue, store, pool, dlq);

  pool.start();
  scheduler.start();
  const server = app.listen(PORT, () => {
    process.stdout.write(`taskflow listening on port ${PORT}\n`);
  });

  const shutdown = (): void => {
    scheduler.stop();
    server.close(() => {
      void pool.stop().then(() => {
        store.close();
        process.exit(0);
      });
    });
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main();
