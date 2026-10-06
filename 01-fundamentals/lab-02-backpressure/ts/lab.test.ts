import { describe, expect, it } from "vitest";
import { eventually } from "@handbook/testkit";
import { BoundedQueue } from "./lab.js";

/** Handler có "cổng": consumer dừng lại ở mỗi message cho tới khi test cho đi tiếp. */
function gatedConsumer<T>() {
  const taken: T[] = [];
  const releases: (() => void)[] = [];
  const handler = (msg: T): Promise<void> => {
    taken.push(msg);
    return new Promise<void>((resolve) => releases.push(resolve));
  };
  return { taken, handler, release: () => releases.shift()?.() };
}

/** Cho mọi microtask và I/O callback đã xếp hàng chạy xong, không dùng timer. */
const flush = () => new Promise<void>((resolve) => setImmediate(resolve));

describe("lab-02 backpressure", () => {
  it("publish_blocks_when_queue_full", async () => {
    for (const capacity of [1, 3]) {
      const queue = new BoundedQueue<number>(capacity);
      for (let i = 0; i < capacity; i++) await queue.publish(i); // lấp đầy queue, không lần publish nào bị chặn
      expect(queue.size()).toBe(capacity);

      let published = false;
      const blocked = queue.publish(capacity).then(() => {
        published = true;
      });

      // Publish đang pending thua cuộc đua với một marker đã resolve, và vẫn pending
      // sau khi event loop quay một vòng: nó thực sự đang chờ chỗ trống.
      const marker = Promise.resolve("still-waiting" as const);
      expect(await Promise.race([blocked.then(() => "published" as const), marker])).toBe(
        "still-waiting",
      );
      await flush();
      expect(published).toBe(false);
      expect(queue.size()).toBe(capacity);

      // Consume một message tạo chỗ trống, và publish đang bị chặn hoàn tất.
      const consumer = gatedConsumer<number>();
      queue.consume(consumer.handler);
      await eventually(async () => published, { timeoutMs: 2000 });
      await blocked;
      expect(consumer.taken).toEqual([0]); // FIFO: message cũ nhất được lấy trước
      expect(queue.size()).toBe(capacity); // chỗ trống vừa giải phóng được publish đang bị chặn lấp lại
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

    // Producer nhanh: 20 lần publish liên tiếp, mỗi lần đều chờ chỗ trống.
    const producer = (async () => {
      for (let i = 0; i < total; i++) {
        await queue.publish(i);
        observe();
      }
    })();

    // Consumer chậm: nó giữ mỗi message cho tới khi test cho đi tiếp.
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
    expect(maxSize).toBe(capacity); // và giới hạn thực sự đã chạm tới, nên phép kiểm tra có ý nghĩa
  });
});
