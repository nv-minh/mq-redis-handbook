// Queue có giới hạn trong bộ nhớ, với backpressure: khi queue đầy, publish() không return
// cho tới khi consumer nhường chỗ. Producer bị làm chậm theo tốc độ của consumer
// thay vì để queue phình ra không giới hạn.

export class BoundedQueue<T> {
  private readonly items: T[] = [];
  private readonly waiting: { msg: T; resolve: () => void }[] = [];
  private handler: ((msg: T) => void | Promise<void>) | undefined;
  private running = false;

  constructor(private readonly capacity: number) {
    if (!Number.isInteger(capacity) || capacity < 1) {
      throw new RangeError(`capacity phải là số nguyên >= 1, nhận ${capacity}`);
    }
  }

  /** Resolve khi message đã nằm trong queue. Chờ (không polling) khi queue đang đầy. */
  publish(msg: T): Promise<void> {
    if (this.items.length < this.capacity) {
      this.items.push(msg);
      void this.pump();
      return Promise.resolve();
    }
    return new Promise<void>((resolve) => this.waiting.push({ msg, resolve }));
  }

  /**
   * Khởi động một consumer tuần tự duy nhất. Message đang được xử lý đã rời queue,
   * nên không tính vào size().
   */
  consume(handler: (msg: T) => void | Promise<void>): void {
    this.handler = handler;
    void this.pump();
  }

  /** Số message đang chờ trong queue, không bao giờ vượt quá capacity. */
  size(): number {
    return this.items.length;
  }

  private async pump(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      while (this.handler !== undefined && this.items.length > 0) {
        const msg = this.items.shift() as T;
        const next = this.waiting.shift(); // chỗ trống vừa giải phóng được trao cho publisher chờ lâu nhất
        if (next !== undefined) {
          this.items.push(next.msg);
          next.resolve();
        }
        try {
          await this.handler(msg);
        } catch {
          // Xử lý lỗi là chủ đề của lab-01; ở đây message lỗi chỉ bị bỏ đi.
        }
      }
    } finally {
      this.running = false;
    }
  }
}
