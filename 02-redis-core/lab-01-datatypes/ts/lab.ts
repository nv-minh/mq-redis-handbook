import type { Redis } from "ioredis";

export interface Entry {
  name: string;
  score: number;
}

/**
 * A leaderboard on one sorted set: member = player name, score = points.
 * ZADD is O(log N), ZRANGE ... REV returns the highest scores first.
 */
export class Leaderboard {
  constructor(
    private readonly redis: Redis,
    private readonly key: string,
  ) {}

  /** Set the score of `name`. Adding an existing name replaces its score (no duplicate member). */
  async add(name: string, score: number): Promise<void> {
    await this.redis.zadd(this.key, score, name);
  }

  /** Names of the `n` highest scores, best first. Empty list for an empty board or n <= 0. */
  async top(n: number): Promise<string[]> {
    // ZRANGE key 0 -1 REV means "everything", so a non-positive n must not reach Redis.
    if (n <= 0) return [];
    return this.redis.zrange(this.key, 0, String(n - 1), "REV");
  }

  /**
   * Same as top(), with scores. Measured against ioredis 6.0.0 (RESP3, Redis 8.10.2): the reply of
   * ZRANGE ... REV WITHSCORES is a FLAT array of strings ["bob", "20", "alice", "10.5"], so the
   * pairs are rebuilt here and the scores parsed from string to number.
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
