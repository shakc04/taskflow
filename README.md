# taskflow

taskflow is a priority job queue that runs handler functions concurrently up to a configurable limit and persists job state to SQLite so that work survives a process restart. Jobs are ordered by a numeric priority where a lower number wins, and the caller decides what that number means for their domain. A priority of 1 might be an urgent email while a priority of 100 is a nightly cleanup; taskflow does not interpret the scale, it only guarantees that lower-numbered jobs are dequeued ahead of higher-numbered ones, with ties broken by creation time so that equal priorities run roughly first in, first out.

The queue itself is a binary min-heap implemented from scratch over a typed `PriorityQueue<T>` that takes a comparator at construction. A worker pool pulls from the queue, marks each job running, invokes the handler, and writes the resulting status back to SQLite. An Express HTTP API sits on top for enqueueing, listing, inspecting, and cancelling jobs. Jobs that fail can be retried with exponential backoff and moved to a dead letter queue when their attempts run out, jobs can be deferred to run at a chosen time, and callers can observe each transition through optional hooks or submit many jobs in one request.

## Installation and build

Requires Node.js 20 or newer.

```
npm install
npm run build
npm test
npm start
```

`npm run build` compiles TypeScript to `dist`. `npm start` runs the compiled server, which reads `PORT`, `CONCURRENCY`, `DB_FILE`, and `POLL_INTERVAL_MS` from the environment with defaults of 3000, 4, `taskflow.sqlite`, and 1000.

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

The HTTP API exposes the same operations. `POST /jobs` takes `{ type, payload, priority }` along with the optional `run_at` and `retry_policy` fields described below, and returns the created job. `GET /jobs` lists every job and accepts an optional `status` query parameter. `GET /jobs/:id` returns a single job. `DELETE /jobs/:id` cancels a job that is still pending and refuses to touch one that is running, done, or failed.

## Retry policy

Each job may carry a retry policy, which is an object with the fields `max_attempts`, `base_delay_ms`, `max_delay_ms`, and `jitter`. When a job has a policy and its handler throws, the pool decrements the attempts that remain and schedules the job to run again after a delay computed as `base_delay_ms` multiplied by two raised to the number of retries already made, with the result held down to `max_delay_ms` so that the backoff does not grow without bound. When `jitter` is enabled the delay is multiplied by a random factor between one half and one so that a group of jobs that failed at the same moment does not retry in unison and overload whatever they depend on. A job that uses up its attempts is removed from the jobs table and written to the dead letter store, and a job with no policy is marked failed on its first error.

## Dead letter queue

The dead letter queue is a separate table that holds jobs which exhausted their retries, recording the original job alongside the reason its last attempt failed and the time it was moved. The HTTP API exposes `GET /dlq` to read every entry and `GET /dlq/:id` to read one, and `POST /dlq/:id/requeue` moves an entry back into the jobs table with its status reset to pending and its attempt budget restored, so that a job which failed because of a transient outage can be tried again once the cause is resolved.

## Deferred scheduling

A job may be given a `run_at` timestamp in Unix milliseconds, supplied as an optional field on the enqueue body and defaulting to the current time, and a job whose `run_at` lies in the future stays in the store as pending without being run. A scheduler polls the store on a fixed interval, one second by default, finds the jobs whose `run_at` has arrived, and hands them to the worker pool. The scheduler polls rather than relying on a database trigger because a trigger fires only when a row changes and cannot report that a moment in time has passed, so the scheduler re-reads the due jobs on each tick and picks up a deferred job once its `run_at` falls behind the current time.

## Event hooks

The worker pool accepts an optional set of hooks, which are callbacks named `onEnqueue`, `onStart`, `onComplete`, and `onFail` that receive the job as it reaches each of those points. The callbacks run for their side effects alone, so the pool does not wait for them and does not let an error they raise reach the job, which means a hook can record a metric or write a log line without changing whether the job itself succeeds.

## Batch enqueue

`POST /jobs/batch` accepts an array of up to one hundred job definitions, each with the same fields as a single enqueue, and returns the ids of the jobs it created. The endpoint validates every item before it writes anything, and if any item is malformed it rejects the whole request with the index and reason of the first failure and creates no jobs, so that a caller never has to work out which part of a partially accepted batch still needs to be sent.

## Test strategy

Tests run under Jest with ts-jest and are split by unit. The heap is tested directly for push and pop ordering, empty behavior, single elements, duplicate priorities, and inverted comparators. The job queue is tested for priority ordering and for the guarantee that `peekNext` does not mutate. The worker pool is tested with `jest.fn` handlers and an in-memory SQLite store so that concurrency limits, the done and failed transitions, retry rescheduling, dead letter handoff, and the hook callbacks can be asserted without standing up long-lived background workers. The retry math is checked at each exponent step and across a thousand jittered samples to confirm the delay stays within its bounds, the scheduler is tested against a mocked clock so that a deferred job runs only after its `run_at` has passed, and the HTTP layer is covered with supertest integration tests that exercise every route including batch enqueue and the dead letter endpoints.

## Known limitations

taskflow runs in a single process and does not coordinate distributed workers, so it does not replace a networked broker when work needs to span machines. SQLite serializes writes, which is fine for the moderate throughput this is built for but becomes a contention point under very high write volume from many concurrent workers. The scheduler discovers due jobs by polling, so a deferred job becomes eligible to run at some point within one poll interval of its `run_at` rather than at the exact millisecond, and shortening the interval to narrow that window costs a query against the store on every tick.
