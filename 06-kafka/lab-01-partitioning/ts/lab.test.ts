import { afterEach, describe, expect, it } from "vitest";
import { eventually, uniqueName } from "@handbook/testkit";
import {
  closeProducer,
  createTopic,
  deleteTopic,
  produceKeyed,
  produceUnkeyed,
  readTopic,
} from "./lab.js";

const PARTITIONS = 3;
const topics: string[] = [];

/** Tạo topic 3 partition và đăng ký tên để afterEach xóa dù test fail. */
async function newTopic(): Promise<string> {
  const topic = uniqueName("lab01-orders");
  // Đăng ký tên trước khi tạo: xóa một topic chưa tồn tại chỉ là no-op.
  topics.push(topic);
  await createTopic(topic, PARTITIONS);
  return topic;
}

afterEach(async () => {
  await closeProducer();
  for (const topic of topics.splice(0)) await deleteTopic(topic);
});

describe("lab-01 partitioning: key, partition và thứ tự", () => {
  it("same_key_always_goes_to_same_partition", async () => {
    const topic = await newTopic();
    const keys = ["alice", "bob", "carol", "dave", "erin"];
    const seen = new Map<string, Set<number>>();

    // Partition lấy từ ack của broker khi produce, không cần consume để biết message nằm ở đâu.
    for (let round = 0; round < 5; round++) {
      for (const key of keys) {
        const { partition } = await produceKeyed(topic, key, `${key}-${round}`);
        expect(partition).toBeGreaterThanOrEqual(0);
        expect(partition).toBeLessThan(PARTITIONS);
        seen.set(key, (seen.get(key) ?? new Set()).add(partition));
      }
    }

    for (const key of keys) {
      expect([...(seen.get(key) ?? [])], `key ${key} phải nằm đúng một partition`).toHaveLength(1);
    }
    // Hash murmur2 là hàm xác định, nên năm key này luôn rơi vào nhiều hơn một partition (đã đo, không phải ngẫu nhiên).
    const used = new Set([...seen.values()].map((partitions) => [...partitions][0]));
    expect(used.size).toBeGreaterThan(1);
  }, 60_000);

  it("order_is_preserved_within_a_partition", async () => {
    const topic = await newTopic();
    const keys = ["a", "b", "c"];
    const produced = new Map<string, { partition: number; offset: bigint; value: string }[]>();

    // Trộn ba key để partition nào cũng có nhiều key; thứ tự chỉ được bảo đảm trong từng partition.
    for (let n = 0; n < 8; n++) {
      for (const key of keys) {
        const value = `${key}-${n}`;
        const { partition, offset } = await produceKeyed(topic, key, value);
        const list = produced.get(key) ?? [];
        list.push({ partition, offset: BigInt(offset), value });
        produced.set(key, list);
      }
    }

    for (const [key, list] of produced) {
      expect(new Set(list.map((r) => r.partition)).size, `key ${key}`).toBe(1);
      for (let i = 1; i < list.length; i++) {
        const previous = list[i - 1];
        const current = list[i];
        expect(current && previous && current.offset > previous.offset).toBe(true);
      }
    }

    // Đọc lại topic từ đầu: với mỗi key, thứ tự value đọc ra phải đúng thứ tự đã produce.
    const records = await readTopic(topic);
    for (const [key, list] of produced) {
      const consumed = records.filter((r) => r.key === key);
      expect(consumed.map((r) => r.value)).toEqual(list.map((r) => r.value));
      expect(consumed.map((r) => BigInt(r.offset))).toEqual(list.map((r) => r.offset));
      expect(new Set(consumed.map((r) => r.partition))).toEqual(new Set([list[0]?.partition]));
    }
  }, 60_000);

  it("null_key_spreads_across_partitions", async () => {
    const topic = await newTopic();
    const partitions = new Set<number>();
    let sent = 0;

    // Key null dùng sticky partitioning: message gửi sát nhau có thể dồn vào cùng một partition,
    // nên không assert phân phối đều. Gửi từng message một cho tới khi thấy ít nhất hai partition khác nhau.
    await eventually(
      async () => {
        const { partition } = await produceUnkeyed(topic, `null-key-${sent++}`);
        partitions.add(partition);
        return partitions.size >= 2 ? partitions.size : undefined;
      },
      { timeoutMs: 45_000, intervalMs: 1 },
    );

    expect(partitions.size).toBeGreaterThanOrEqual(2);
  }, 60_000);
});
