export interface BackoffOptions {
  /** Cận trên của delay ở attempt 0, tính bằng mili giây. Phải hữu hạn và lớn hơn 0. */
  baseMs: number;
  /** Trần của cận trên, tính bằng mili giây. Phải hữu hạn và lớn hơn 0. */
  capMs: number;
}

/**
 * Cận trên của delay ở lần retry thứ `attempt`: min(capMs, baseMs * 2^attempt).
 *
 * `2 ** attempt` với attempt lớn là Infinity (2 ** 1100), và Math.min(capMs, Infinity) vẫn ra capMs,
 * nên không cần tự chặn số mũ. Hàm ném lỗi khi tham số không hợp lệ thay vì trả NaN hoặc số âm.
 */
export function backoffBound(attempt: number, { baseMs, capMs }: BackoffOptions): number {
  if (!Number.isInteger(attempt) || attempt < 0) {
    throw new Error(`attempt phải là số nguyên không âm, nhận được ${attempt}`);
  }
  if (!Number.isFinite(baseMs) || baseMs <= 0) {
    throw new Error(`baseMs phải hữu hạn và lớn hơn 0, nhận được ${baseMs}`);
  }
  if (!Number.isFinite(capMs) || capMs <= 0) {
    throw new Error(`capMs phải hữu hạn và lớn hơn 0, nhận được ${capMs}`);
  }
  return Math.min(capMs, baseMs * 2 ** attempt);
}

/**
 * Full jitter của AWS Architecture Blog: `rand() * min(capMs, baseMs * 2^attempt)`.
 *
 * Hàm thuần: nguồn ngẫu nhiên là tham số `rand` (trả về số trong [0, 1), ví dụ Math.random hoặc
 * một PRNG có seed), nên test điều khiển được kết quả. Kết quả nằm trong [0, backoffBound).
 * Không làm tròn: caller tự làm tròn nếu cần số nguyên mili giây.
 */
export function backoffDelay(attempt: number, opts: BackoffOptions, rand: () => number): number {
  const bound = backoffBound(attempt, opts);
  const r = rand();
  // rand ngoài [0, 1) sẽ phá cận trên (delay âm hoặc lớn hơn bound), nên từ chối thay vì âm thầm kẹp lại.
  if (!(r >= 0 && r < 1)) {
    throw new Error(`rand() phải trả về số trong [0, 1), nhận được ${r}`);
  }
  return r * bound;
}
