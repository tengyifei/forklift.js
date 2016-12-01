import * as Bluebird from 'bluebird';

/**
 * Means the process could not complete because another party tries to cancel it.
 */
export class InterruptedError extends Error {
  constructor(message: string) {
    super(message);
  }
}

/**
 * Promise-based Semaphore implementation.
 */
export default class Semaphore {

  max: number;
  count: number;
  queue: Array<{ resolve: () => void, interrupt: () => void }>;

  /**
   * Creates new Semaphore instance.
   * @param {Integer} max - Maximum capacity of the Semaphore
   */
  constructor(max?: number) {
    this.max = max || 1;
    this.count = this.max;
    this.queue = [];
  }

  release(): void {
    this.count++;
    if (this.count > this.max) this.count = this.max;
    if (this.queue.length === 0) return;
    this.queue.shift().resolve();   // resolves the promise
  }

  acquire(): Bluebird<void> {
    this.count--;
    if (this.count >= 0) {
      return Bluebird.resolve();   // did not hit backlog
    }
    return new Bluebird<void>((resolve, reject) => {
      this.queue.push({
        resolve: () => resolve(),
        interrupt: () => reject(new InterruptedError('Interruped'))
      });
    });
  }

  autoAcquire(): Bluebird.Disposer<void> {
    return this.acquire()
    .disposer(() => this.release());    // use with bluebird's `using` function
  }

  /**
   * Stops all operations waiting on this Semaphore
   */
  cancel(): void {
    this.queue.forEach(elem => elem.interrupt());   // interrupt all outstanding Promises
    if (this.count < 0) this.count = 0;
    this.queue = [];
  }
}
