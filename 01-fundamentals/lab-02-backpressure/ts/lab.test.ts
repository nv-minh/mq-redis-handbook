import { describe, expect, it } from "vitest";
import { eventually } from "@handbook/testkit";
import { BoundedQueue } from "./lab.js";

/** A handler gate: the consumer parks on each message until the test releases it. */
function gatedConsumer<T>() {
  const taken: T[] = [];
  const releases: (() => void)[] = [];
  const handler = (msg: T): Promise<void> => {
    taken.push(msg);
    return new Promise<void>((resolve) => releases.push(resolve));
  };
  return { taken, handler, release: () => releases.shift()?.() };
}

/** Let every already-queued microtask and I/O callback run, without a timer. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

describe("lab-02 backpressure", () => {
  it("publish_blocks_when_queue_full", async () => {
    for (const capacity of [1, 3]) {
      const queue = new BoundedQueue<number>(capacity);
      for (let i = 0; i < capacity; i++) await queue.publish(i); // fills it, none of these block
      expect(queue.size()).toBe(capacity);

      let published = false;
      const blocked = queue.publish(capacity).then(() => {
        published = true;
      });

      // The pending publish loses a race against an already-resolved marker, and stays pending
      // after the event loop has turned: it is genuinely waiting for room.
      const marker = Promise.resolve("still-waiting" as const);
      expect(await Promise.race([blocked.then(() => "published" as const), marker])).toBe(
        "still-waiting",
      );
      await flush();
      expect(published).toBe(false);
      expect(queue.size()).toBe(capacity);

      // Consuming one message makes room, and the blocked publish completes.
      const consumer = gatedConsumer<number>();
      queue.consume(consumer.handler);
      await eventually(async () => published, { timeoutMs: 2000 });
      await blocked;
      expect(consumer.taken).toEqual([0]); // FIFO: the oldest message was taken first
      expect(queue.size()).toBe(capacity); // the freed slot was refilled by the blocked publish
    }
  });

  it("publish_rejects_a_capacity_below_one", () => {
    expect(() => new BoundedQueue<number>(0)).toThrow(RangeError);
    expect(() => new BoundedQueue<number>(1.5)).toThrow(RangeError);
  });

  it("size_never_exceeds_capacity_with_slow_consumer", async () => {
    const capacity = 3;
    const total = 20;
    const queue = new BoundedQueue<number>(capacity);
    let maxSize = 0;
    const observe = () => {
      maxSize = Math.max(maxSize, queue.size());
    };

    // A fast producer: 20 publishes issued back to back, each one waits for room.
    const producer = (async () => {
      for (let i = 0; i < total; i++) {
        await queue.publish(i);
        observe();
      }
    })();

    // A slow consumer: it holds each message until the test lets it go.
    const consumer = gatedConsumer<number>();
    queue.consume((msg) => {
      observe();
      return consumer.handler(msg);
    });

    for (let done = 0; done < total; done++) {
      await eventually(async () => consumer.taken.length > done, {
        timeoutMs: 2000,
        intervalMs: 1,
      });
      observe();
      consumer.release();
    }
    await producer;

    expect(consumer.taken).toEqual(Array.from({ length: total }, (_, i) => i));
    expect(maxSize).toBeLessThanOrEqual(capacity);
    expect(maxSize).toBe(capacity); // and the bound was actually reached, so the check has teeth
  });
});
