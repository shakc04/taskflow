import { JobQueue } from '../src/core/queue';
import { Job } from '../src/core/job';

function makeJob(id: string, priority: number, createdAt: number): Job {
  return {
    id,
    type: 'test',
    payload: {},
    priority,
    status: 'pending',
    created_at: createdAt,
    started_at: null,
    finished_at: null,
    error: null,
  };
}

describe('JobQueue', () => {
  it('dequeues the lowest priority number first', () => {
    const queue = new JobQueue();
    queue.enqueue(makeJob('a', 5, 1));
    queue.enqueue(makeJob('b', 1, 2));
    queue.enqueue(makeJob('c', 3, 3));

    expect(queue.dequeue()?.id).toBe('b');
    expect(queue.dequeue()?.id).toBe('c');
    expect(queue.dequeue()?.id).toBe('a');
    expect(queue.dequeue()).toBeNull();
  });

  it('breaks ties by creation time so equal priorities stay first in first out', () => {
    const queue = new JobQueue();
    queue.enqueue(makeJob('first', 2, 100));
    queue.enqueue(makeJob('second', 2, 200));
    queue.enqueue(makeJob('third', 2, 300));

    expect(queue.dequeue()?.id).toBe('first');
    expect(queue.dequeue()?.id).toBe('second');
    expect(queue.dequeue()?.id).toBe('third');
  });

  it('peekNext returns the head without removing it', () => {
    const queue = new JobQueue();
    queue.enqueue(makeJob('a', 4, 1));
    queue.enqueue(makeJob('b', 2, 2));

    expect(queue.peekNext()?.id).toBe('b');
    expect(queue.size).toBe(2);
    expect(queue.peekNext()?.id).toBe('b');
    expect(queue.dequeue()?.id).toBe('b');
    expect(queue.size).toBe(1);
  });

  it('returns null from peekNext and dequeue when empty', () => {
    const queue = new JobQueue();
    expect(queue.peekNext()).toBeNull();
    expect(queue.dequeue()).toBeNull();
  });
});
