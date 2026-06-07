import { PriorityQueue } from '../src/core/heap';

const ascending = (a: number, b: number): number => a - b;

describe('PriorityQueue', () => {
  it('returns null when popping an empty heap', () => {
    const heap = new PriorityQueue<number>(ascending);
    expect(heap.pop()).toBeNull();
    expect(heap.peek()).toBeNull();
    expect(heap.isEmpty()).toBe(true);
  });

  it('handles a single element', () => {
    const heap = new PriorityQueue<number>(ascending);
    heap.push(42);
    expect(heap.size).toBe(1);
    expect(heap.peek()).toBe(42);
    expect(heap.pop()).toBe(42);
    expect(heap.isEmpty()).toBe(true);
  });

  it('pops elements in ascending comparator order regardless of push order', () => {
    const heap = new PriorityQueue<number>(ascending);
    for (const value of [5, 1, 4, 2, 8, 3, 7, 6]) {
      heap.push(value);
    }
    const popped: number[] = [];
    while (!heap.isEmpty()) {
      const next = heap.pop();
      if (next !== null) {
        popped.push(next);
      }
    }
    expect(popped).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
  });

  it('keeps min behavior as elements are interleaved with pushes and pops', () => {
    const heap = new PriorityQueue<number>(ascending);
    heap.push(10);
    heap.push(3);
    expect(heap.pop()).toBe(3);
    heap.push(1);
    heap.push(7);
    expect(heap.pop()).toBe(1);
    expect(heap.pop()).toBe(7);
    expect(heap.pop()).toBe(10);
    expect(heap.pop()).toBeNull();
  });

  it('returns all elements when priorities are duplicated', () => {
    const heap = new PriorityQueue<number>(ascending);
    for (const value of [2, 2, 1, 1, 3, 3]) {
      heap.push(value);
    }
    const popped: number[] = [];
    while (!heap.isEmpty()) {
      const next = heap.pop();
      if (next !== null) {
        popped.push(next);
      }
    }
    expect(popped).toEqual([1, 1, 2, 2, 3, 3]);
    expect(popped).toHaveLength(6);
  });

  it('acts as a max-heap when the comparator is inverted', () => {
    const heap = new PriorityQueue<number>((a, b) => b - a);
    for (const value of [4, 9, 1, 7]) {
      heap.push(value);
    }
    expect(heap.pop()).toBe(9);
    expect(heap.pop()).toBe(7);
  });
});
