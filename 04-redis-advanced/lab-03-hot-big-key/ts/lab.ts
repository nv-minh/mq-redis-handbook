import type { Redis } from "ioredis";

/**
 * Find keys whose memory footprint is at least `thresholdBytes`, biggest first.
 *
 * It walks the keyspace with SCAN (cursor based, never blocks Redis the way KEYS does) and asks
 * MEMORY USAGE for the keys of each page in one pipeline. `match` is a glob that SCAN applies
 * on the server, `*` scans every key. Nothing is deleted and no configuration is changed.
 *
 * MEMORY USAGE counts the key, its value and allocator overhead, so a key is "big" by bytes
 * here, not by element count (which is what `redis-cli --bigkeys` reports).
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
      // null: the key expired or was deleted between SCAN and MEMORY USAGE.
      if (typeof bytes === "number" && bytes >= thresholdBytes)
        found.push({ key: keys[i]!, bytes });
    });
  } while (cursor !== "0");
  // SCAN may return a key more than once.
  const unique = new Map(found.map((f) => [f.key, f]));
  return [...unique.values()].sort((a, b) => b.bytes - a.bytes).map((f) => f.key);
}
