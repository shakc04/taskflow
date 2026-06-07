/**
 * Binary min-heap. The element that the comparator ranks lowest sits at the
 * root and comes out first. To get max-heap behavior, pass a comparator that
 * inverts the usual ordering.
 *
 * The comparator follows the standard contract: it returns a negative number
 * when `a` should rank ahead of `b`, a positive number when `b` should rank
 * ahead of `a`, and zero when they tie.
 */
export class PriorityQueue<T> {
  private readonly items: T[] = [];

  constructor(private readonly compare: (a: T, b: T) => number) {}

  get size(): number {
    return this.items.length;
  }

  isEmpty(): boolean {
    return this.items.length === 0;
  }

  push(value: T): void {
    this.items.push(value);
    this.siftUp(this.items.length - 1);
  }

  /**
   * Returns the root without removing it, or null when the heap is empty.
   */
  peek(): T | null {
    if (this.items.length === 0) {
      return null;
    }
    // length check above guarantees index 0 is populated
    return this.items[0] as T;
  }

  /**
   * Removes and returns the root, or null when the heap is empty.
   */
  pop(): T | null {
    if (this.items.length === 0) {
      return null;
    }
    const root = this.items[0] as T;
    const last = this.items.pop() as T;
    if (this.items.length > 0) {
      this.items[0] = last;
      this.siftDown(0);
    }
    return root;
  }

  private siftUp(start: number): void {
    let index = start;
    const value = this.items[index] as T;
    while (index > 0) {
      const parent = (index - 1) >> 1;
      const parentValue = this.items[parent] as T;
      if (this.compare(value, parentValue) >= 0) {
        break;
      }
      this.items[index] = parentValue;
      index = parent;
    }
    this.items[index] = value;
  }

  private siftDown(start: number): void {
    let index = start;
    const length = this.items.length;
    const value = this.items[index] as T;
    while (true) {
      const left = index * 2 + 1;
      const right = left + 1;
      let smallest = index;
      let smallestValue = value;

      if (left < length) {
        const leftValue = this.items[left] as T;
        if (this.compare(leftValue, smallestValue) < 0) {
          smallest = left;
          smallestValue = leftValue;
        }
      }
      if (right < length) {
        const rightValue = this.items[right] as T;
        if (this.compare(rightValue, smallestValue) < 0) {
          smallest = right;
          smallestValue = rightValue;
        }
      }
      if (smallest === index) {
        break;
      }
      this.items[index] = smallestValue;
      index = smallest;
    }
    this.items[index] = value;
  }
}
