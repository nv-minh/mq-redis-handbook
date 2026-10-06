import type { Redis } from "ioredis";

export interface ReliableQueueOptions {
  /** Connection for normal commands (LPUSH, LREM, LMOVE, LRANGE). */
  redis: Redis;
  /** A connection used ONLY for BLMOVE: a blocked connection cannot serve other commands. */
  blocking: Redis;
  /** Key of the queue list. Processing lists live under `<queue>:processing:<consumerId>`. */
  queue: string;
  /** How long dequeue waits on an empty queue, in seconds (BLMOVE timeout, fractions allowed). */
  blockTimeoutSeconds?: number;
}

/**
 * A reliable queue on Redis lists.
 *
 * enqueue: LPUSH queue msg (new messages enter at the left, consumers take from the right: FIFO).
 * dequeue: BLMOVE queue processing:<consumer> RIGHT LEFT, one atomic step that takes the oldest
 *          message AND parks it in the consumer's processing list, so a crash cannot lose it.
 * ack:     LREM processing:<consumer> 1 msg, the message is done.
 * recoverStale: move what a dead consumer left in its processing list back onto the queue.
 *
 * Delivery is at-least-once: a message recovered from a consumer that was only slow is processed twice.
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

  /** Key of the list that holds the messages `consumerId` has taken but not acked. */
  processingKey(consumerId: string): string {
    return `${this.queue}:processing:${consumerId}`;
  }

  async enqueue(msg: string): Promise<void> {
    await this.redis.lpush(this.queue, msg);
  }

  /**
   * Take the oldest message, blocking up to the block timeout. Resolves null when the queue stayed
   * empty for the whole timeout (BLMOVE replies nil). The message stays in the processing list
   * of `consumerId` until ack().
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

  /** Remove `msg` from the processing list of `consumerId`. Resolves false if it was not there. */
  async ack(consumerId: string, msg: string): Promise<boolean> {
    return (await this.redis.lrem(this.processingKey(consumerId), 1, msg)) === 1;
  }

  /**
   * Put every message in the processing list of `consumerId` back on the queue, and resolve with
   * how many were moved (0 for an empty list). Call it for a consumer you know is dead.
   *
   * Each LMOVE is atomic, so a crash of the recovering process cannot lose a message.
   * LEFT -> RIGHT: the processing list holds the newest message at its left end, so moving from
   * its left end to the right end of the queue (the end consumers read from) leaves the oldest
   * message last in line to be pushed, therefore first to be consumed again: original order kept.
   */
  async recoverStale(consumerId: string): Promise<number> {
    const processing = this.processingKey(consumerId);
    let moved = 0;
    while ((await this.redis.lmove(processing, this.queue, "LEFT", "RIGHT")) !== null) moved++;
    return moved;
  }
}
