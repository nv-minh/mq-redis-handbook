import { randomBytes } from "node:crypto";
import type { Redis } from "ioredis";

export interface StreamMessage {
  id: string;
  fields: Record<string, string>;
}

/** Một lần giao message cho handler. `deliveryCount` là số lần entry đã được giao (lần đầu là 1). */
export interface Delivery extends StreamMessage {
  /** Tên consumer (worker) đang giữ message này. */
  consumer: string;
  deliveryCount: number;
}

/** Handler xử lý một message. Ném lỗi (hoặc reject) nghĩa là lần xử lý này thất bại và message không được ack. */
export type Handler = (delivery: Delivery) => Promise<void>;

export interface PendingEntry {
  id: string;
  consumer: string;
  idleMs: number;
  deliveryCount: number;
}

export interface WorkerGroupOptions {
  /**
   * Connection cho các lệnh thường (XADD, XACK, XAUTOCLAIM, XPENDING, XGROUP), do caller sở hữu.
   * Mỗi worker lấy một connection blocking riêng bằng `redis.duplicate()` và đóng nó trong `stop()`.
   */
  redis: Redis;
  /** Stream chứa công việc. */
  stream: string;
  /** Consumer group mà các worker cùng đọc. */
  group: string;
  /** XAUTOCLAIM chỉ tiếp quản entry đã pending lâu hơn ngưỡng này (mili giây). */
  minIdleMs: number;
  /** Thời gian mỗi worker chờ entry mới trong một lần XREADGROUP. Cũng là độ trễ tối đa của `stop()`. Mặc định 1000. */
  blockMs?: number;
  /** Được gọi khi handler lỗi. Mặc định bỏ qua: message nằm lại PEL và sẽ được claim lại. */
  onError?: (error: unknown, delivery: Delivery) => void;
}

export interface WorkerHandle {
  /** Tên consumer của từng worker (duy nhất, mỗi worker một tên). */
  readonly consumerNames: readonly string[];
  /**
   * Dừng nhận việc mới, chờ handler đang chạy xong (và ack nó), rồi đóng các connection blocking.
   * Idempotent. Reject nếu một worker đã chết vì lỗi Redis (sau khi đã dọn xong mọi connection).
   */
  stop(): Promise<void>;
}

/**
 * Competing consumers trên Redis Streams: n worker cùng một consumer group, mỗi message chỉ giao cho
 * một worker. Worker ack sau khi handler thành công, nên delivery là at-least-once: handler lỗi hoặc worker
 * chết giữa chừng thì message nằm lại trong pending list (PEL) và một worker còn sống tiếp quản bằng XAUTOCLAIM.
 */
export class WorkerGroup {
  private readonly redis: Redis;
  private readonly stream: string;
  private readonly group: string;
  private readonly minIdleMs: number;
  private readonly blockMs: number;
  private readonly onError: (error: unknown, delivery: Delivery) => void;

  constructor(options: WorkerGroupOptions) {
    this.redis = options.redis;
    this.stream = options.stream;
    this.group = options.group;
    this.minIdleMs = options.minIdleMs;
    this.blockMs = options.blockMs ?? 1000;
    this.onError = options.onError ?? (() => undefined);
  }

  /** XGROUP CREATE ... MKSTREAM từ id 0. Idempotent: lỗi BUSYGROUP bị nuốt. */
  async ensureGroup(): Promise<void> {
    try {
      await this.redis.xgroup("CREATE", this.stream, this.group, "0", "MKSTREAM");
    } catch (error) {
      if (!(error instanceof Error && error.message.startsWith("BUSYGROUP"))) throw error;
    }
  }

  /** XADD vào stream, resolve với id của entry. */
  async publish(fields: Record<string, string>): Promise<string> {
    const id = await this.redis.xadd(this.stream, "*", ...Object.entries(fields).flat());
    if (id === null) throw new Error("XADD không trả về id");
    return id;
  }

  /** Pending list của group, cũ nhất trước (XPENDING dạng extended). */
  async pendingEntries(count = 1000): Promise<PendingEntry[]> {
    const rows = (await this.redis.xpending(this.stream, this.group, "-", "+", count)) as [
      string,
      string,
      number,
      number,
    ][];
    return rows.map(([id, consumer, idleMs, deliveryCount]) => ({
      id,
      consumer,
      idleMs,
      deliveryCount,
    }));
  }

  /**
   * Khởi động n worker. Mỗi worker có tên consumer duy nhất và một connection riêng cho XREADGROUP BLOCK
   * (connection đang bị chặn không phục vụ được lệnh khác). Mỗi vòng lặp của worker:
   *
   * 1. XAUTOCLAIM tiếp quản một entry đã pending quá `minIdleMs` (của worker đã chết, hoặc lần xử lý trước lỗi).
   * 2. Nếu không có, XREADGROUP ... > đọc entry mới, chặn tối đa `blockMs`.
   * 3. Chạy handler. Thành công thì XACK; lỗi thì không ack.
   *
   * Handler phải chạy xong trong khoảng ngắn hơn `minIdleMs`, nếu không worker khác sẽ tiếp quản message
   * khi handler đầu còn đang chạy. Lab này không giới hạn số lần thử: message lỗi mãi sẽ bị thử lại mãi
   * (xem lab 03 cho DLQ).
   */
  startWorkers(n: number, handler: Handler): WorkerHandle {
    if (!Number.isInteger(n) || n < 1) {
      throw new Error(`n phải là số nguyên dương, nhận được ${n}`);
    }
    const prefix = `worker-${randomBytes(4).toString("hex")}`;
    const consumerNames = Array.from({ length: n }, (_, i) => `${prefix}-${i}`);
    const blockingConnections = consumerNames.map(() => this.redis.duplicate());
    const state = { stopping: false };
    const failures: unknown[] = [];

    // .catch ngay lúc tạo: một worker chết vì lỗi Redis không được thành unhandled rejection
    // trước khi caller gọi stop(). Lỗi được báo ở stop().
    const loops = consumerNames.map((consumer, i) =>
      this.runWorker(consumer, blockingConnections[i]!, state, handler).catch((error: unknown) => {
        failures.push(error);
      }),
    );

    let stopping: Promise<void> | undefined;
    const stop = (): Promise<void> => {
      stopping ??= (async () => {
        state.stopping = true;
        try {
          await Promise.all(loops);
        } finally {
          for (const connection of blockingConnections) connection.disconnect();
        }
        if (failures.length > 0) throw failures[0];
      })();
      return stopping;
    };
    return { consumerNames, stop };
  }

  private async runWorker(
    consumer: string,
    blocking: Redis,
    state: { stopping: boolean },
    handler: Handler,
  ): Promise<void> {
    while (!state.stopping) {
      const delivery = (await this.claimOne(consumer)) ?? (await this.readNew(blocking, consumer));
      if (delivery === undefined) continue;
      try {
        await handler(delivery);
      } catch (error) {
        // Không ack: message nằm lại PEL cho tới khi một worker claim lại sau minIdleMs.
        this.onError(error, delivery);
        continue;
      }
      await this.redis.xack(this.stream, this.group, delivery.id);
    }
  }

  /**
   * XAUTOCLAIM theo cursor cho tới khi claim được một entry (hoặc cursor về 0-0). Mỗi lần chỉ claim một entry,
   * để các entry còn lại không bị reset idle time rồi nằm chờ trong lúc handler chạy.
   * Delivery count đọc từ XPENDING sau khi claim (claim cộng 1).
   */
  private async claimOne(consumer: string): Promise<Delivery | undefined> {
    let cursor = "0-0";
    do {
      const [next, entries] = (await this.redis.xautoclaim(
        this.stream,
        this.group,
        consumer,
        this.minIdleMs,
        cursor,
        "COUNT",
        1,
      )) as [string, [string, string[] | null][], string[] | undefined];
      const entry = entries.find((e): e is [string, string[]] => e[1] !== null);
      if (entry) {
        const deliveryCount = await this.deliveryCount(entry[0]);
        if (deliveryCount !== undefined) {
          return { ...toMessage(entry), consumer, deliveryCount };
        }
      }
      cursor = next;
    } while (cursor !== "0-0");
    return undefined;
  }

  /** XREADGROUP ... > : entry chưa từng được giao cho group nên delivery count là 1. */
  private async readNew(blocking: Redis, consumer: string): Promise<Delivery | undefined> {
    const reply = (await blocking.xreadgroup(
      "GROUP",
      this.group,
      consumer,
      "COUNT",
      1,
      "BLOCK",
      this.blockMs,
      "STREAMS",
      this.stream,
      ">",
    )) as [string, [string, string[]][]][] | null;
    const entry = reply?.[0]?.[1][0];
    if (entry === undefined) return undefined;
    return { ...toMessage(entry), consumer, deliveryCount: 1 };
  }

  /** XPENDING dạng extended cho đúng một id. undefined khi entry không còn trong PEL. */
  private async deliveryCount(id: string): Promise<number | undefined> {
    const rows = (await this.redis.xpending(this.stream, this.group, id, id, 1)) as [
      string,
      string,
      number,
      number,
    ][];
    return rows[0]?.[3];
  }
}

function toMessage([id, flat]: [string, string[]]): StreamMessage {
  const fields: Record<string, string> = {};
  for (let i = 0; i + 1 < flat.length; i += 2) fields[flat[i] as string] = flat[i + 1] as string;
  return { id, fields };
}
