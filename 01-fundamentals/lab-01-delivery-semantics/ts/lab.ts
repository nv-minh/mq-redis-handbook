// Broker trong bộ nhớ để cho thấy "delivery semantics" nghĩa là gì, không dùng mạng thật.
// Một message đi qua pending -> được giao cho handler -> (có thể) được ack. Việc mất message được
// tạo ra qua `lossRate` và `rand`, nên mọi lỗi trong lab đều tái hiện được.

export type Mode = "at-most-once" | "at-least-once";

export interface QueueOptions {
  mode: Mode;
  /**
   * Xác suất (0..1) có sự cố trên đường truyền:
   * - at-most-once: lần giao bị mất, handler không bao giờ thấy message.
   * - at-least-once: ack của consumer bị mất, nên broker giao lại (sinh ra duplicate).
   * 1 nghĩa là luôn xảy ra, 0 nghĩa là không bao giờ.
   */
  lossRate: number;
  /** Nguồn ngẫu nhiên trong [0, 1). Mặc định là Math.random. */
  rand?: () => number;
  /** Chỉ at-least-once: số lần giao tối đa trước khi message bị cho vào dead. Mặc định 5. */
  maxDeliveries?: number;
}

export interface Queue {
  publish(id: string): void;
  /**
   * Đăng ký consumer. Handler ném lỗi mô phỏng consumer bị crash:
   * at-most-once đã quên message, at-least-once không nhận được ack nên thử giao lại.
   */
  consume(handler: (id: string) => void): void;
  /** Resolve khi mọi message có thể giao đã ngã ngũ (được ack, bị mất hoặc vào dead). */
  drain(): Promise<void>;
  /** Các id đã dùng hết maxDeliveries mà vẫn chưa có ack. */
  deadLetters(): string[];
}

export function createQueue(opts: QueueOptions): Queue {
  const rand = opts.rand ?? Math.random;
  const maxDeliveries = opts.maxDeliveries ?? 5;
  const pending: string[] = [];
  const deliveries = new Map<string, number>();
  const dead: string[] = [];
  let handler: ((id: string) => void) | undefined;
  let pump: Promise<void> | undefined;

  const deliver = (id: string, handle: (id: string) => void): void => {
    if (opts.mode === "at-most-once") {
      // Gửi rồi quên: broker làm rơi message trước hoặc trong lúc giao.
      if (rand() < opts.lossRate) return;
      try {
        handle(id);
      } catch {
        // Consumer crash, nhưng không còn gì để thử lại: message đã mất rồi.
      }
      return;
    }

    const attempt = (deliveries.get(id) ?? 0) + 1;
    deliveries.set(id, attempt);
    let acked = false;
    try {
      handle(id);
      acked = rand() >= opts.lossRate; // ack đi ngược về broker và có thể bị mất
    } catch {
      // Consumer crash: cũng không có ack.
    }
    if (acked) {
      deliveries.delete(id);
    } else if (attempt >= maxDeliveries) {
      deliveries.delete(id);
      dead.push(id);
    } else {
      pending.push(id); // giao lại, xếp sau các message đang chờ
    }
  };

  const run = async (): Promise<void> => {
    await Promise.resolve(); // để một loạt lời gọi publish() kịp vào hàng đợi trước khi giao
    while (handler !== undefined && pending.length > 0) {
      deliver(pending.shift() as string, handler);
    }
    pump = undefined;
  };

  const kick = (): void => {
    if (pump === undefined && handler !== undefined && pending.length > 0) pump = run();
  };

  return {
    publish(id) {
      pending.push(id);
      kick();
    },
    consume(h) {
      handler = h;
      kick();
    },
    async drain() {
      while (pump !== undefined) await pump;
    },
    deadLetters: () => [...dead],
  };
}

/**
 * Bọc `apply` để bỏ qua các id được giao lại. Id được ghi lại trước khi apply chạy và bị xóa
 * nếu apply ném lỗi, nếu không một lần thử thất bại sẽ bị coi là duplicate mãi mãi.
 * Chỉ trong bộ nhớ: consumer thật phải lưu marker nguyên tử cùng với side effect.
 */
export function createIdempotentHandler(apply: (id: string) => void): (id: string) => void {
  const seen = new Set<string>();
  return (id) => {
    if (seen.has(id)) return;
    seen.add(id);
    try {
      apply(id);
    } catch (error) {
      seen.delete(id);
      throw error;
    }
  };
}
