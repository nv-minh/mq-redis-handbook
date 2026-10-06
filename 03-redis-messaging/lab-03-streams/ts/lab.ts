import type { Redis } from "ioredis";

export interface StreamMessage {
  id: string;
  fields: Record<string, string>;
}

export interface PendingEntry {
  id: string;
  /** Consumer đang sở hữu entry. */
  consumer: string;
  /** Số mili giây kể từ lần entry được giao gần nhất. */
  idleMs: number;
  /** Số lần entry đã được giao (một lần claim cũng tính là một lần giao). */
  deliveryCount: number;
}

export interface ClaimResult {
  /** Các entry mà lời gọi này đã tiếp quản, kèm field của chúng. */
  messages: StreamMessage[];
  /** Các id còn trong pending list nhưng không còn tồn tại trong stream (Redis 7.0+). */
  deletedIds: string[];
}

export interface StreamQueueOptions {
  /** Connection cho các lệnh thường (XADD, XACK, XAUTOCLAIM, XPENDING, XGROUP). */
  redis: Redis;
  /** Connection chỉ dùng cho XREADGROUP ... BLOCK: connection đang bị chặn không phục vụ được lệnh khác. */
  blocking: Redis;
  /** Key của stream. */
  stream: string;
  /** Thời gian consume chờ entry mới, tính bằng mili giây. Mặc định 1000. */
  blockMs?: number;
}

/** Mỗi lần gọi, XAUTOCLAIM quét và claim tối đa chừng này entry. Đây là mặc định của server. */
const CLAIM_PAGE = 100;

/**
 * Work queue trên một Redis Stream với consumer group.
 *
 * publish: XADD stream * field value ...
 * consume: XREADGROUP GROUP g c COUNT n BLOCK ms STREAMS stream >   (các entry chưa từng được giao cho group)
 * ack:     XACK stream g id                                          (xóa id khỏi pending list)
 * claimStale: XAUTOCLAIM, tiếp quản các entry mà consumer khác để pending quá lâu.
 *
 * Delivery là at-least-once: một entry nằm trong pending list (PEL) của group cho tới khi được ack.
 */
export class StreamQueue {
  private readonly redis: Redis;
  private readonly blocking: Redis;
  private readonly stream: string;
  private readonly blockMs: number;

  constructor(options: StreamQueueOptions) {
    this.redis = options.redis;
    this.blocking = options.blocking;
    this.stream = options.stream;
    this.blockMs = options.blockMs ?? 1000;
  }

  /** Thêm một entry và resolve với id của nó (`<ms>-<seq>`, do server sinh ra). */
  async publish(fields: Record<string, string>): Promise<string> {
    const args = Object.entries(fields).flat();
    const id = await this.redis.xadd(this.stream, "*", ...args);
    if (id === null) throw new Error("XADD không trả về id");
    return id;
  }

  /**
   * XGROUP CREATE ... MKSTREAM: tạo group (và tạo stream nếu chưa có). `startId` "0"
   * (mặc định) khiến group thấy toàn bộ stream, "$" chỉ thấy các entry được thêm từ bây giờ.
   * Idempotent: lỗi BUSYGROUP (group đã tồn tại) bị nuốt, mọi lỗi khác được ném ra.
   */
  async createGroup(group: string, startId = "0"): Promise<void> {
    try {
      await this.redis.xgroup("CREATE", this.stream, group, startId, "MKSTREAM");
    } catch (error) {
      if (!(error instanceof Error && error.message.startsWith("BUSYGROUP"))) throw error;
    }
  }

  /**
   * Đọc tối đa `count` entry chưa từng được giao cho group này, chặn tối đa bằng block time.
   * Chúng vào pending list của `consumer`. Resolve [] khi không có gì tới (reply là nil).
   * ioredis 6.0.0 trên RESP3 trả về [[stream, [[id, [field, value, ...]]]]] (hoặc null).
   */
  async consume(group: string, consumer: string, count: number): Promise<StreamMessage[]> {
    const reply = (await this.blocking.xreadgroup(
      "GROUP",
      group,
      consumer,
      "COUNT",
      count,
      "BLOCK",
      this.blockMs,
      "STREAMS",
      this.stream,
      ">",
    )) as [string, [string, string[]][]][] | null;
    if (reply === null) return [];
    return reply.flatMap(([, entries]) => entries.map(toMessage));
  }

  /** XACK: resolve với số id đã được xóa khỏi pending list (0 nếu đã ack từ trước). */
  ack(group: string, id: string): Promise<number> {
    return this.redis.xack(this.stream, group, id);
  }

  /**
   * Tiếp quản mọi entry của group đã pending lâu hơn `minIdleMs`, bất kể ai đang sở hữu,
   * và biến `consumer` thành chủ sở hữu. Đi theo cursor của XAUTOCLAIM cho tới khi nó trả về 0-0.
   * Claim đặt lại idle time và cộng thêm một vào delivery count.
   * Resolve { messages: [], deletedIds: [] } khi không có entry nào đủ điều kiện.
   *
   * ioredis 6.0.0 trả về đủ reply 3 phần tử: [cursor kế tiếp, entries, các id đã xóa].
   */
  async claimStale(group: string, consumer: string, minIdleMs: number): Promise<ClaimResult> {
    const result: ClaimResult = { messages: [], deletedIds: [] };
    let cursor = "0-0";
    do {
      const [next, entries, deleted] = (await this.redis.xautoclaim(
        this.stream,
        group,
        consumer,
        minIdleMs,
        cursor,
        "COUNT",
        CLAIM_PAGE,
      )) as [string, [string, string[] | null][], string[] | undefined];
      result.messages.push(...entries.filter(hasFields).map(toMessage));
      result.deletedIds.push(...(deleted ?? []));
      cursor = next;
    } while (cursor !== "0-0");
    return result;
  }

  /** Số entry trong pending list của group (XPENDING dạng summary). */
  async pendingCount(group: string): Promise<number> {
    // Reply: [count, id nhỏ nhất, id lớn nhất, [[consumer, "count"], ...]], và [0, null, null, null] khi rỗng.
    const summary = (await this.redis.xpending(this.stream, group)) as [number, ...unknown[]];
    return summary[0];
  }

  /** Pending list của group, cũ nhất trước (XPENDING dạng extended). */
  async pendingEntries(group: string, count = 1000): Promise<PendingEntry[]> {
    // Reply: [[id, consumer, idle ms, delivery count], ...]
    const rows = (await this.redis.xpending(this.stream, group, "-", "+", count)) as [
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
}

function toMessage([id, flat]: [string, string[] | null]): StreamMessage {
  const fields: Record<string, string> = {};
  const list = flat ?? [];
  for (let i = 0; i + 1 < list.length; i += 2) fields[list[i] as string] = list[i + 1] as string;
  return { id, fields };
}

function hasFields(entry: [string, string[] | null]): entry is [string, string[]] {
  return entry[1] !== null;
}
