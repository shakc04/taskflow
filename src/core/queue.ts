import { PriorityQueue } from './heap';
import { Job } from './job';

/**
 * In-memory ordering of pending jobs. There is no locking here: Node runs this
 * code on a single thread, so enqueue and dequeue can never interleave. The
 * worker pool relies on that to read peekNext and then dequeue without another
 * caller slipping in between.
 */
export class JobQueue {
  private readonly heap: PriorityQueue<Job>;

  constructor() {
    // Lower priority value should come out first, so compare ascending on
    // priority. Ties fall back to creation time to keep ordering stable and
    // roughly first-in-first-out among equal priorities.
    this.heap = new PriorityQueue<Job>((a, b) => {
      if (a.priority !== b.priority) {
        return a.priority - b.priority;
      }
      return a.created_at - b.created_at;
    });
  }

  get size(): number {
    return this.heap.size;
  }

  enqueue(job: Job): void {
    this.heap.push(job);
  }

  dequeue(): Job | null {
    return this.heap.pop();
  }

  peekNext(): Job | null {
    return this.heap.peek();
  }
}
