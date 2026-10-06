import { describe, expect, it } from "vitest";
import { backoffBound, backoffDelay, type BackoffOptions } from "./lab.js";

const opts: BackoffOptions = { baseMs: 100, capMs: 1_000_000 };

/** PRNG có seed (mulberry32): test lặp lại được và không phụ thuộc Math.random. */
function seeded(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** Số thực lớn nhất nhỏ hơn 1, để thử biên trên của rand. */
const JUST_BELOW_ONE = 1 - Number.EPSILON / 2;

describe("lab-02 retry backoff: full jitter", () => {
  it("delay_is_within_zero_and_exponential_bound", () => {
    const rand = seeded(42);
    for (let attempt = 0; attempt <= 8; attempt++) {
      const bound = opts.baseMs * 2 ** attempt;
      for (let i = 0; i < 500; i++) {
        const delay = backoffDelay(attempt, opts, rand);
        expect(delay).toBeGreaterThanOrEqual(0);
        expect(delay).toBeLessThan(bound);
      }
      // rand điều khiển được: delay đúng bằng rand * bound.
      expect(backoffDelay(attempt, opts, () => 0)).toBe(0);
      expect(backoffDelay(attempt, opts, () => 0.5)).toBe(bound / 2);
      const nearMax = backoffDelay(attempt, opts, () => JUST_BELOW_ONE);
      expect(nearMax).toBeLessThan(bound);
      expect(nearMax).toBeGreaterThan(bound * 0.999999);
    }
  });

  it("delay_never_exceeds_cap", () => {
    const capped: BackoffOptions = { baseMs: 100, capMs: 5_000 };
    const rand = seeded(7);
    for (const attempt of [6, 10, 30, 52, 100]) {
      for (let i = 0; i < 200; i++) {
        expect(backoffDelay(attempt, capped, rand)).toBeLessThan(capped.capMs);
      }
      // Khi 2^attempt * base đã vượt cap thì cận trên là chính cap.
      expect(backoffBound(attempt, capped)).toBe(capped.capMs);
      expect(backoffDelay(attempt, capped, () => 0.5)).toBe(capped.capMs / 2);
    }
  });

  it("attempt_zero_uses_base", () => {
    expect(backoffBound(0, opts)).toBe(opts.baseMs);
    expect(backoffDelay(0, opts, () => 0.5)).toBe(opts.baseMs / 2);
    expect(backoffDelay(0, opts, () => 0)).toBe(0);
    expect(backoffDelay(0, opts, () => JUST_BELOW_ONE)).toBeLessThan(opts.baseMs);
    // Cap nhỏ hơn base thì cap thắng ngay từ attempt 0.
    expect(backoffBound(0, { baseMs: 100, capMs: 30 })).toBe(30);
  });

  it("very_large_attempt_does_not_overflow", () => {
    const capped: BackoffOptions = { baseMs: 100, capMs: 5_000 };
    // 2 ** 1100 là Infinity trong JS: Math.min với cap vẫn cho kết quả hữu hạn.
    for (const attempt of [1023, 1024, 1100, 1_000_000, Number.MAX_SAFE_INTEGER]) {
      expect(backoffBound(attempt, capped)).toBe(5_000);
      const delay = backoffDelay(attempt, capped, () => 0.25);
      expect(Number.isFinite(delay)).toBe(true);
      expect(delay).toBe(1_250);
    }
  });

  it("delays_spread_across_the_whole_range_instead_of_clustering", () => {
    // Đây là lý do có jitter: 2000 client cùng retry lần 5 không nhận cùng một delay.
    const rand = seeded(2026);
    const bound = backoffBound(5, opts);
    const delays = Array.from({ length: 2000 }, () => backoffDelay(5, opts, rand));
    expect(Math.min(...delays)).toBeLessThan(bound * 0.01);
    expect(Math.max(...delays)).toBeGreaterThan(bound * 0.99);
    const mean = delays.reduce((a, b) => a + b, 0) / delays.length;
    expect(mean).toBeGreaterThan(bound * 0.45);
    expect(mean).toBeLessThan(bound * 0.55);
    expect(new Set(delays).size).toBeGreaterThan(1990);
  });

  it("invalid_inputs_are_rejected_with_a_clear_error", () => {
    const rand = () => 0.5;
    expect(() => backoffDelay(-1, opts, rand)).toThrow(/attempt/);
    expect(() => backoffDelay(1.5, opts, rand)).toThrow(/attempt/);
    expect(() => backoffDelay(Number.NaN, opts, rand)).toThrow(/attempt/);
    expect(() => backoffDelay(0, { baseMs: 0, capMs: 1000 }, rand)).toThrow(/baseMs/);
    expect(() => backoffDelay(0, { baseMs: -5, capMs: 1000 }, rand)).toThrow(/baseMs/);
    expect(() => backoffDelay(0, { baseMs: Infinity, capMs: 1000 }, rand)).toThrow(/baseMs/);
    expect(() => backoffDelay(0, { baseMs: 100, capMs: 0 }, rand)).toThrow(/capMs/);
    expect(() => backoffDelay(0, { baseMs: 100, capMs: Number.NaN }, rand)).toThrow(/capMs/);
    // rand phải nằm trong [0, 1): giá trị ngoài khoảng sẽ phá cận trên.
    expect(() => backoffDelay(0, opts, () => 1)).toThrow(/rand/);
    expect(() => backoffDelay(0, opts, () => -0.1)).toThrow(/rand/);
    expect(() => backoffDelay(0, opts, () => Number.NaN)).toThrow(/rand/);
  });
});
