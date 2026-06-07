# taskflow

taskflow is a priority job queue that runs handler functions concurrently up to a configurable limit and persists job state to SQLite so that work survives a process restart. Jobs are ordered by a numeric priority where a lower number wins, and the caller decides what that number means for their domain. A priority of 1 might be an urgent email while a priority of 100 is a nightly cleanup; taskflow does not interpret the scale, it only guarantees that lower-numbered jobs are dequeued ahead of higher-numbered ones, with ties broken by creation time so that equal priorities run roughly first in, first out.

The queue itself is a binary min-heap implemented from scratch over a typed `PriorityQueue<T>` that takes a comparator at construction. A worker pool pulls from the queue, marks each job running, invokes the handler, and writes the resulting status back to SQLite. An Express HTTP API sits on top for enqueueing, listing, inspecting, and cancelling jobs.

## Installation and build

Requires Node.js 20 or newer.

```
npm install
npm run build
npm test
npm start
```

`npm run build` compiles TypeScript to `dist`. `npm start` runs the compiled server, which reads `PORT`, `CONCURRENCY`, and `DB_FILE` from the environment with defaults of 3000, 4, and `taskflow.sqlite`.

## Usage

The pieces are wired together by hand so the data flow stays visible:

```ts
import { JobQueue } from './core/queue';
import { JobStore } from './store/sqlite';
import { WorkerPool } from './core/worker-pool';

const store = new JobStore('taskflow.sqlite');
const queue = new JobQueue();

const pool = new WorkerPool(queue, store, 4, async (job) => {
  // Replace this with domain work keyed on job.type.
  await sendEmail(job.payload);
});

pool.start();

const job = buildJob({ type: 'email', payload: { to: 'a@b.com' }, priority: 1 });
store.insert(job);
queue.enqueue(job);
pool.notify();
```

The HTTP API exposes the same operations. `POST /jobs` takes `{ type, payload, priority }` and returns the created job. `GET /jobs` lists every job and accepts an optional `status` query parameter. `GET /jobs/:id` returns a single job. `DELETE /jobs/:id` cancels a job that is still pending and refuses to touch one that is running, done, or failed.

## Test strategy

Tests run under Jest with ts-jest and are split by unit. The heap is tested directly for push and pop ordering, empty behavior, single elements, duplicate priorities, and inverted comparators. The job queue is tested for priority ordering and for the guarantee that `peekNext` does not mutate. The worker pool is tested with `jest.fn` handlers and an in-memory SQLite store so that concurrency limits, the done transition, the failed transition, and cancellation can be asserted without standing up long-lived background workers. The HTTP layer is covered with supertest integration tests that exercise the happy path of every route plus the validation and not-found branches.

## Known limitations

taskflow runs in a single process and does not coordinate distributed workers, so it does not replace a networked broker when work needs to span machines. SQLite serializes writes, which is fine for the moderate throughput this is built for but becomes a contention point under very high write volume from many concurrent workers. There is no retry logic; a job whose handler throws is marked failed and stays failed until something re-enqueues it.
