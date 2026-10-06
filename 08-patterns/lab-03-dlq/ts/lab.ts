import type { Redis } from "ioredis";

export interface StreamMessage {
  id: string;
  fields: Record<string, string>;
}

/** Một lần giao message cho handler. `deliveryCount` là số lần entry đã được giao (lần đầu là 1). */
export interface Delivery extends StreamMessage {
  deliveryCount: number;
}

/** Handler xử lý một message. Ném lỗi (hoặc reject) nghĩa là lần xử lý này thất bại. */
export type Handler = (delivery: Delivery) => Promise<void>;

export interface PendingEntry {
  id: string;
  consumer: string;
  idleMs: number;
  deliveryCount: number;
}

export interface DlqQueueOptions {
  /** Connection cho các lệnh thường. */
  redis: Redis;
  /** Connection chỉ dùng cho XREADGROUP ... BLOCK. */
  blocking: Redis;
  /** Stream chính. */
  stream: string;
  /** Tên consumer group. */
  group: string;
  /** Stream chứa các message đã bị loại (dead-letter). */
  dlqStream: string;
  /** Tên consumer của instance này (duy nhất trong group). */
  consumer: string;
  /** XAUTOCLAIM chỉ tiếp quản entry đã pending lâu hơn ngưỡng này (mili giây). */
  minIdleMs: number;
  /** Thời gian chờ entry mới mỗi vòng. Mặc định 1000. */
  blockMs?: number;
}

export interface ConsumeOptions {
  /** Tổng số lần handler được chạy cho một message trước khi nó bị chuyển vào DLQ. Số nguyên dương. */
  maxAttempts: number;
  /** Hủy vòng consume (kiểm tra mỗi vòng). Khi hủy, hàm resolve với thống kê tới lúc đó. */
  signal?: AbortSignal;
}

export interface ConsumeStats {
  /** Số lần handler thành công (entry đã được XACK). */
  succeeded: number;
  /** Số lần handler thất bại. */
  failed: number;
  /** Số message đã chuyển vào DLQ. */
  deadLettered: number;
}

/** Đọc từng message một: message đang chờ trong batch không bị tính idle trong lúc handler trước còn chạy. */
const READ_COUNT = 1;
const CLAIM_PAGE = 100;

/**
 * Consumer trên Redis Streams có retry và dead-letter queue.
 *
 * Redis Streams không có DLQ sẵn (suy ra từ danh sách lệnh trong docs), nên pattern tự cài:
 * message lỗi nằm lại trong pending list (PEL), được tiếp quản lại bằng XAUTOCLAIM sau `minIdleMs`,
 * delivery count lấy từ XPENDING (dạng extended). Khi count chạm `maxAttempts` thì consumer
 * XADD message vào DLQ stream kèm metadata rồi XACK entry gốc.
 */
export class DlqQueue {
  private readonly redis: Redis;
  private readonly blocking: Redis;
  private readonly stream: string;
  private readonly group: string;
  private readonly dlqStream: string;
  private readonly consumer: string;
  private readonly minIdleMs: number;
  private readonly blockMs: number;

  constructor(options: DlqQueueOptions) {
    this.redis = options.redis;
    this.blocking = options.blocking;
    this.stream = options.stream;
    this.group = options.group;
    this.dlqStream = options.dlqStream;
    this.consumer = options.consumer;
    this.minIdleMs = options.minIdleMs;
    this.blockMs = options.blockMs ?? 1000;
  }

  /** XGROUP CREATE ... MKSTREAM từ id 0. Idempotent: lỗi BUSYGROUP bị nuốt. */
  async ensureGroup(): Promise<void> {
    try {
      await this.redis.xgroup("CREATE", this.stream, this.group, "0", "MKSTREAM");
    } catch (error) {
      if (!(error instanceof Error && error.message.startsWith("BUSYGROUP"))) throw error;
    }
  }

  /** XADD vào stream chính, resolve với id của entry. */
  async publish(fields: Record<string, string>): Promise<string> {
    const id = await this.redis.xadd(this.stream, "*", ...Object.entries(fields).flat());
    if (id === null) throw new Error("XADD không trả về id");
    return id;
  }

  /**
   * Chạy vòng consume cho tới khi hết việc: stream không còn entry mới VÀ pending list của group rỗng
   * (hoặc `signal` bị hủy). Mỗi vòng:
   *
   * 1. XAUTOCLAIM tiếp quản entry pending quá `minIdleMs` (của consumer đã chết, hoặc lần thử trước đã lỗi).
   *    Lần claim cộng 1 vào delivery count. Entry có count lớn hơn `maxAttempts` đã bị giao đủ số lần
   *    mà không ai báo kết quả (consumer chết giữa chừng), nên vào DLQ mà không chạy handler,
   *    để một message làm chết consumer không lặp vô hạn.
   * 2. XREADGROUP ... > đọc entry mới (delivery count 1), chặn tối đa `blockMs`.
   * 3. Handler thành công thì XACK. Handler lỗi thì entry nằm lại trong PEL; nếu delivery count
   *    (đọc từ XPENDING) đã chạm `maxAttempts` thì chuyển vào DLQ rồi XACK.
   *
   * Handler phải chạy xong trong khoảng ngắn hơn `minIdleMs`, nếu không consumer khác sẽ tiếp quản
   * message khi handler đầu còn đang chạy (cùng một đánh đổi với visibility timeout của SQS).
   */
  async consumeWithDlq(opts: ConsumeOptions, handler: Handler): Promise<ConsumeStats> {
    const { maxAttempts, signal } = opts;
    if (!Number.isInteger(maxAttempts) || maxAttempts < 1) {
      throw new Error(`maxAttempts phải là số nguyên dương, nhận được ${maxAttempts}`);
    }
    const stats: ConsumeStats = { succeeded: 0, failed: 0, deadLettered: 0 };

    while (!signal?.aborted) {
      let worked = false;
      for (const delivery of await this.claimStale()) {
        worked = true;
        if (delivery.deliveryCount > maxAttempts) {
          const reason =
            `consumer đã chết hoặc bị kill trước khi báo lỗi ` +
            `(delivery count ${delivery.deliveryCount} vượt maxAttempts ${maxAttempts})`;
          await this.deadLetter(delivery, reason, delivery.deliveryCount);
          stats.deadLettered += 1;
        } else {
          await this.handle(delivery, maxAttempts, handler, stats);
        }
      }
      for (const delivery of await this.readNew()) {
        worked = true;
        await this.handle(delivery, maxAttempts, handler, stats);
      }
      if (!worked && (await this.pendingCount()) === 0) return stats;
    }
    return stats;
  }

  /**
   * Chuyển tối đa `limit` entry cũ nhất từ DLQ về stream chính (field gốc, entry mới có id mới)
   * và resolve với số entry thực sự đã chuyển (0 khi DLQ rỗng, kể cả khi limit lớn hơn kích thước DLQ).
   *
   * Thứ tự an toàn: XADD vào stream chính TRƯỚC, XDEL khỏi DLQ SAU. Nếu process chết giữa hai lệnh
   * thì entry nằm ở cả hai nơi và lần replay sau sẽ chuyển nó lần nữa (trùng, at-least-once), chứ không mất.
   * Đảo thứ tự (XDEL trước) thì crash giữa chừng làm mất message. Vì vậy consumer phải idempotent
   * theo business id nằm trong payload, không theo id của stream.
   */
  async replayDlq(limit: number): Promise<number> {
    if (!Number.isInteger(limit) || limit < 1) {
      throw new Error(`limit phải là số nguyên dương, nhận được ${limit}`);
    }
    const entries = await this.dlqEntries(limit);
    let moved = 0;
    for (const entry of entries) {
      const payload = parsePayload(entry);
      await this.redis.xadd(this.stream, "*", ...Object.entries(payload).flat());
      await this.redis.xdel(this.dlqStream, entry.id);
      moved += 1;
    }
    return moved;
  }

  /** Các entry của DLQ, cũ nhất trước (XRANGE - +). */
  async dlqEntries(count = 1000): Promise<StreamMessage[]> {
    const rows = (await this.redis.xrange(this.dlqStream, "-", "+", "COUNT", count)) as [
      string,
      string[],
    ][];
    return rows.map(toMessage);
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

  private async handle(
    delivery: Delivery,
    maxAttempts: number,
    handler: Handler,
    stats: ConsumeStats,
  ): Promise<void> {
    try {
      await handler(delivery);
    } catch (error) {
      stats.failed += 1;
      // Delivery count lấy từ server (XPENDING), không tự đếm ở client.
      const count = await this.deliveryCount(delivery.id);
      if (count !== undefined && count >= maxAttempts) {
        await this.deadLetter(delivery, errorMessage(error), count);
        stats.deadLettered += 1;
      }
      return;
    }
    await this.redis.xack(this.stream, this.group, delivery.id);
    stats.succeeded += 1;
  }

  /**
   * XADD vào DLQ rồi XACK entry gốc, hai lệnh riêng. Nếu process chết giữa chúng thì entry gốc vẫn
   * pending, được claim lại và vào DLQ lần nữa: DLQ có thể có bản trùng (at-least-once), không mất message.
   */
  private async deadLetter(delivery: Delivery, error: string, attempts: number): Promise<void> {
    await this.redis.xadd(
      this.dlqStream,
      "*",
      "original_id",
      delivery.id,
      "error",
      error,
      "attempts",
      String(attempts),
      "failed_at",
      new Date().toISOString(),
      "payload",
      JSON.stringify(delivery.fields),
    );
    await this.redis.xack(this.stream, this.group, delivery.id);
  }

  /** XAUTOCLAIM theo cursor cho tới 0-0. Delivery count của từng entry lấy từ XPENDING sau khi claim. */
  private async claimStale(): Promise<Delivery[]> {
    const claimed: StreamMessage[] = [];
    let cursor = "0-0";
    do {
      const [next, entries] = (await this.redis.xautoclaim(
        this.stream,
        this.group,
        this.consumer,
        this.minIdleMs,
        cursor,
        "COUNT",
        CLAIM_PAGE,
      )) as [string, [string, string[] | null][], string[] | undefined];
      claimed.push(...entries.filter(hasFields).map(toMessage));
      cursor = next;
    } while (cursor !== "0-0");

    const deliveries: Delivery[] = [];
    for (const message of claimed) {
      const deliveryCount = await this.deliveryCount(message.id);
      if (deliveryCount !== undefined) deliveries.push({ ...message, deliveryCount });
    }
    return deliveries;
  }

  /** XREADGROUP ... > : entry chưa từng được giao cho group nên delivery count là 1. */
  private async readNew(): Promise<Delivery[]> {
    const reply = (await this.blocking.xreadgroup(
      "GROUP",
      this.group,
      this.consumer,
      "COUNT",
      READ_COUNT,
      "BLOCK",
      this.blockMs,
      "STREAMS",
      this.stream,
      ">",
    )) as [string, [string, string[]][]][] | null;
    if (reply === null) return [];
    return reply.flatMap(([, entries]) =>
      entries.map((entry) => ({ ...toMessage(entry), deliveryCount: 1 })),
    );
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

  /** Số entry trong PEL của group (XPENDING dạng summary). */
  private async pendingCount(): Promise<number> {
    const summary = (await this.redis.xpending(this.stream, this.group)) as [number, ...unknown[]];
    return summary[0];
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

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function parsePayload(entry: StreamMessage): Record<string, string> {
  try {
    const payload: unknown = JSON.parse(entry.fields.payload ?? "");
    if (typeof payload === "object" && payload !== null && !Array.isArray(payload)) {
      return payload as Record<string, string>;
    }
  } catch {
    // rơi xuống lỗi bên dưới
  }
  throw new Error(`DLQ entry ${entry.id} có field payload không hợp lệ, không replay được`);
}
