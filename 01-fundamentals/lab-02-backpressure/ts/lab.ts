// A bounded in-memory queue with backpressure: when the queue is full, publish() does not
// return until a consumer has made room. The producer is slowed to the consumer's pace
// instead of the queue growing without limit.

export class BoundedQueue<T> {
  private readonly items: T[] = [];
  private readonly waiting: { msg: T; resolve: () => void }[] = [];
  private handler: ((msg: T) => void | Promise<void>) | undefined;
  private running = false;

  constructor(private readonly capacity: number) {
    if (!Number.isInteger(capacity) || capacity < 1) {
      throw new RangeError(`capacity must be an integer >= 1, got ${capacity}`);
    }
  }

  /** Resolves once the message is in the queue. Waits (without polling) while the queue is full. */
  publish(msg: T): Promise<void> {
    if (this.items.length < this.capacity) {
      this.items.push(msg);
      void this.pump();
      return Promise.resolve();
    }
    return new Promise<void>((resolve) => this.waiting.push({ msg, resolve }));
  }

  /**
   * Start a single sequential consumer. The message being handled has already left the queue,
   * so it does not count towards size().
   */
  consume(handler: (msg: T) => void | Promise<void>): void {
    this.handler = handler;
    void this.pump();
  }

  /** Messages waiting in the queue, never more than the capacity. */
  size(): number {
    return this.items.length;
  }

  private async pump(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      while (this.handler !== undefined && this.items.length > 0) {
        const msg = this.items.shift() as T;
        const next = this.waiting.shift(); // the freed slot goes to the longest-waiting publisher
        if (next !== undefined) {
          this.items.push(next.msg);
          next.resolve();
        }
        try {
          await this.handler(msg);
        } catch {
          // Failure handling is the topic of lab-01; here a failed message is just dropped.
        }
      }
    } finally {
      this.running = false;
    }
  }
}
