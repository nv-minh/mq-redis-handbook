import type { Redis } from "ioredis";

/** Kết quả của một lần xử lý: effect đã chạy ("applied") hay message này đã được thấy rồi ("duplicate"). */
export type ProcessResult = "applied" | "duplicate";

/** Key dedup của một message id. Id phải ổn định qua mọi lần redeliver (id do producer gán, không phải id của transport). */
export function dedupKey(messageId: string): string {
  return `idempotency:${messageId}`;
}

/**
 * Idempotent consumer bằng một lệnh Redis: `SET key 1 NX EX ttlSeconds`.
 *
 * - SET NX là atomic set-if-absent: trong N lời gọi đồng thời cùng một id, đúng một lời gọi nhận "OK"
 *   và chạy `effect`, những lời gọi còn lại nhận nil và trả "duplicate" (không check-rồi-set riêng).
 * - Nếu `effect` ném lỗi, key bị xóa (nhả) rồi lỗi gốc được ném lại, để lần redeliver sau còn áp dụng được.
 *   Nếu không nhả, key đã chiếm sẽ khiến mọi lần retry bị coi là trùng và effect bị mất.
 * - Key sống `ttlSeconds` giây sau khi thành công. TTL phải lớn hơn cửa sổ mà broker còn có thể
 *   redeliver (và cả replay từ DLQ), vì khi key hết hạn một bản trùng đến muộn sẽ được áp dụng lại.
 *
 * Giới hạn đã biết (xem README): trong lúc effect đầu còn chạy, bản trùng nhận "duplicate" và consumer
 * thường sẽ ack nó. Nếu effect đầu rồi lỗi và nhả key thì bản trùng đã ack đó không ai xử lý nữa.
 */
export async function processOnce(
  rdb: Redis,
  messageId: string,
  effect: () => Promise<void>,
  ttlSeconds: number,
): Promise<ProcessResult> {
  if (!Number.isInteger(ttlSeconds) || ttlSeconds <= 0) {
    throw new Error(`ttlSeconds phải là số nguyên dương (SET EX), nhận được ${ttlSeconds}`);
  }
  const key = dedupKey(messageId);
  const claimed = await rdb.set(key, "1", "EX", ttlSeconds, "NX");
  if (claimed === null) return "duplicate";

  try {
    await effect();
  } catch (error) {
    // Nhả key. Nếu DEL cũng lỗi (mất kết nối) thì key tự hết hạn sau TTL; lỗi của effect vẫn là lỗi được báo.
    await rdb.del(key).catch(() => undefined);
    throw error;
  }
  return "applied";
}
