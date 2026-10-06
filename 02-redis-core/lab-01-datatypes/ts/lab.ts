import type { Redis } from "ioredis";

export interface Entry {
  name: string;
  score: number;
}

/**
 * Bảng xếp hạng trên một sorted set: member = tên người chơi, score = điểm.
 * ZADD có độ phức tạp O(log N), ZRANGE ... REV trả về điểm cao nhất trước.
 */
export class Leaderboard {
  constructor(
    private readonly redis: Redis,
    private readonly key: string,
  ) {}

  /** Đặt score của `name`. Thêm một tên đã có sẽ thay score của nó (không sinh member trùng). */
  async add(name: string, score: number): Promise<void> {
    await this.redis.zadd(this.key, score, name);
  }

  /** Tên của `n` điểm cao nhất, cao nhất trước. Trả về list rỗng nếu bảng rỗng hoặc n <= 0. */
  async top(n: number): Promise<string[]> {
    // ZRANGE key 0 -1 REV nghĩa là "tất cả", nên n không dương không được gửi tới Redis.
    if (n <= 0) return [];
    return this.redis.zrange(this.key, 0, String(n - 1), "REV");
  }

  /**
   * Giống top(), kèm score. Đã đo với ioredis 6.0.0 (RESP3, Redis 8.10.2): reply của
   * ZRANGE ... REV WITHSCORES là một mảng PHẲNG các string ["bob", "20", "alice", "10.5"], nên
   * ở đây ta ghép lại thành cặp và parse score từ string sang number.
   */
  async topWithScores(n: number): Promise<Entry[]> {
    if (n <= 0) return [];
    const flat = await this.redis.zrange(this.key, 0, String(n - 1), "REV", "WITHSCORES");
    const entries: Entry[] = [];
    for (let i = 0; i + 1 < flat.length; i += 2) {
      entries.push({ name: flat[i] as string, score: Number(flat[i + 1]) });
    }
    return entries;
  }
}
