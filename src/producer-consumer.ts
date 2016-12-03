import { Observable, ReplaySubject, Subscription } from 'rxjs/Rx';

export class ReactiveQueue <T> {
  private stream: ReplaySubject<T>;
  private queue: T[];
  private waiting: boolean;
  private listeners: ((x: T) => PromiseLike<void>)[];
  private unsub: Subscription[];

  get _queue() { return this.queue.slice(); }

  constructor() {
    this.reset();
  }

  reset() {
    this.stream = new ReplaySubject<T>();
    this.queue = [];
    this.listeners = [];
    this.unsub = [];
    this.waiting = false;
  }

  subscribe <R> (onNext: (x: T) => PromiseLike<void>) {
    this.listeners.push(onNext);
    let u = this.stream.subscribe(t => {
      // wait for all consumers
      Promise.all(this.listeners.map(l => l(t)))
      .then(_ => {
        // consumers have processed current element
        this.queue.shift();
        // trigger next element if possible
        if (this.queue.length > 0) {
          this.waiting = true;
          this.stream.next(this.queue[0]);
        } else {
          this.waiting = false;
        }
      });
    });
    this.unsub.push(u);
  }

  push(t: T) {
    this.queue.push(t);
    if (this.waiting) {
      // if consumer is busy, cache it and do nothing
    } else {
      this.waiting = true;
      this.stream.next(t);
    }
  }
}
