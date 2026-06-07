import { Job } from './job';

/**
 * Observation points the worker pool calls as a job moves through its life
 * cycle. Every callback is optional and receives the job in the state it holds
 * at that transition. Callbacks may be synchronous or return a promise; either
 * way the pool does not wait on them.
 */
export interface JobHooks {
  onEnqueue?: (job: Job) => void | Promise<void>;
  onStart?: (job: Job) => void | Promise<void>;
  onComplete?: (job: Job) => void | Promise<void>;
  onFail?: (job: Job) => void | Promise<void>;
}
