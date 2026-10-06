import type { Redis } from "ioredis";

export interface ReliableQueueOptions {
  /** Connection cho các lệnh thường (LPUSH, LREM, LMOVE, LRANGE). */
  redis: Redis;
  /** Connection chỉ dùng cho BLMOVE: connection đang bị chặn không phục vụ được lệnh khác. */
  blocking: Redis;
  /** Key của list queue. Các processing list nằm dưới `<queue>:processing:<consumerId>`. */
  queue: string;
  /** Thời gian dequeue chờ trên queue rỗng, tính bằng giây (timeout của BLMOVE, cho phép số lẻ). */
  blockTimeoutSeconds?: number;
}

/**
 * Queue đáng tin cậy trên Redis list.
 *
 * enqueue: LPUSH queue msg (message mới vào bên trái, consumer lấy từ bên phải: FIFO).
 * dequeue: BLMOVE queue processing:<consumer> RIGHT LEFT, một bước atomic vừa lấy message cũ nhất
 *          VỪA cất nó vào processing list của consumer, nên crash không làm mất message.
 * ack:     LREM processing:<consumer> 1 msg, message đã xong.
 * recoverStale: chuyển những gì consumer đã chết để lại trong processing list về lại queue.
 *
 * Delivery là at-least-once: message được recover từ một consumer chỉ chậm chứ chưa chết sẽ được xử lý hai lần.
 */
export class ReliableQueue {
  private readonly redis: Redis;
  private readonly blocking: Redis;
  private readonly queue: string;
  private readonly blockTimeoutSeconds: number;

  constructor(options: ReliableQueueOptions) {
    this.redis = options.redis;
    this.blocking = options.blocking;
    this.queue = options.queue;
    this.blockTimeoutSeconds = options.blockTimeoutSeconds ?? 1;
  }

  /** Key của list giữ các message mà `consumerId` đã lấy nhưng chưa ack. */
  processingKey(consumerId: string): string {
    return `${this.queue}:processing:${consumerId}`;
  }

  async enqueue(msg: string): Promise<void> {
    await this.redis.lpush(this.queue, msg);
  }

  /**
   * Lấy message cũ nhất, chặn tối đa bằng block timeout. Resolve null khi queue rỗng suốt
   * thời gian timeout (BLMOVE trả về nil). Message nằm trong processing list
   * của `consumerId` cho tới khi ack().
   */
  dequeue(consumerId: string): Promise<string | null> {
    return this.blocking.blmove(
      this.queue,
      this.processingKey(consumerId),
      "RIGHT",
      "LEFT",
      this.blockTimeoutSeconds,
    );
  }

  /** Xóa `msg` khỏi processing list của `consumerId`. Resolve false nếu nó không có ở đó. */
  async ack(consumerId: string, msg: string): Promise<boolean> {
    return (await this.redis.lrem(this.processingKey(consumerId), 1, msg)) === 1;
  }

  /**
   * Đưa mọi message trong processing list của `consumerId` về lại queue, và resolve với
   * số message đã chuyển (0 nếu list rỗng). Gọi hàm này cho consumer mà bạn biết đã chết.
   *
   * Mỗi LMOVE là atomic, nên tiến trình recover có crash cũng không làm mất message.
   * LEFT -> RIGHT: processing list giữ message mới nhất ở đầu bên trái, nên chuyển từ đầu bên trái
   * của nó sang đầu bên phải của queue (đầu mà consumer đọc) khiến message cũ nhất được push
   * sau cùng, do đó được consume đầu tiên trở lại: giữ nguyên thứ tự ban đầu.
   */
  async recoverStale(consumerId: string): Promise<number> {
    const processing = this.processingKey(consumerId);
    let moved = 0;
    while ((await this.redis.lmove(processing, this.queue, "LEFT", "RIGHT")) !== null) moved++;
    return moved;
  }
}
