import type { Redis } from "ioredis";

/**
 * Tìm các key có kích thước bộ nhớ từ `thresholdBytes` trở lên, key lớn nhất đứng đầu.
 *
 * Hàm duyệt keyspace bằng SCAN (theo cursor, không chặn Redis như KEYS) và hỏi MEMORY USAGE
 * cho các key của mỗi trang trong một pipeline.
 * `match` là glob mà SCAN áp dụng ngay trên server, `*` là quét mọi key.
 * Hàm không xóa gì và không đổi cấu hình server nào.
 *
 * MEMORY USAGE tính cả key, value và overhead của allocator, nên "big" ở đây là theo byte,
 * không phải theo số phần tử (cái mà `redis-cli --bigkeys` báo).
 */
export async function findBigKeys(
  rdb: Redis,
  thresholdBytes: number,
  match = "*",
): Promise<string[]> {
  const found: { key: string; bytes: number }[] = [];
  let cursor = "0";
  do {
    const [next, keys] = await rdb.scan(cursor, "MATCH", match, "COUNT", 200);
    cursor = next;
    if (keys.length === 0) continue;
    const pipeline = rdb.pipeline();
    for (const key of keys) pipeline.call("MEMORY", "USAGE", key);
    const replies = (await pipeline.exec()) ?? [];
    replies.forEach(([error, bytes], i) => {
      if (error) throw error;
      // null: key đã hết hạn hoặc bị xóa giữa lúc SCAN và MEMORY USAGE, bỏ qua thay vì báo lỗi.
      if (typeof bytes === "number" && bytes >= thresholdBytes)
        found.push({ key: keys[i]!, bytes });
    });
  } while (cursor !== "0");
  // SCAN có thể trả cùng một key nhiều lần nên loại trùng.
  const unique = new Map(found.map((f) => [f.key, f]));
  return [...unique.values()].sort((a, b) => b.bytes - a.bytes).map((f) => f.key);
}
