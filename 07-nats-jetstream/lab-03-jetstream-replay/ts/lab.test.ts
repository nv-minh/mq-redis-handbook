import { afterEach, describe, expect, it } from "vitest";
import { uniqueName } from "@handbook/testkit";
import {
  createReplayStream,
  openReplay,
  parseNanos,
  publishBatch,
  startTimeBetween,
  storedMessageTime,
  teardown,
} from "./lab.js";

const names: string[] = [];

/** Đăng ký tên trước khi tạo để afterEach luôn dọn, kể cả khi bước tạo bị lỗi giữa chừng. */
async function newStream(): Promise<string> {
  const name = uniqueName("lab03");
  names.push(name);
  await createReplayStream(name);
  return name;
}

afterEach(async () => {
  // allSettled: một teardown lỗi không được làm các stream còn lại bị bỏ sót. Lỗi đầu tiên vẫn được ném ra.
  const results = await Promise.allSettled(names.splice(0).map((name) => teardown(name)));
  const failed = results.find((r) => r.status === "rejected");
  if (failed) throw (failed as PromiseRejectedResult).reason;
});

const bodies = (count: number, prefix: string) =>
  Array.from({ length: count }, (_, i) => `${prefix}-${i}`);

describe("lab-03 JetStream replay: deliver policy", () => {
  it("deliver_all_replays_history", async () => {
    const total = 8;
    const name = await newStream();
    const published = bodies(total, "event");
    await publishBatch(name, published);

    // Consumer MỚI với DeliverAll đọc lại từ message đầu tiên, đúng thứ tự stream.
    const first = await (await openReplay(name, { kind: "all" })).read();
    expect(first.map((m) => m.body)).toEqual(published);
    expect(first.map((m) => m.seq)).toEqual(Array.from({ length: total }, (_, i) => i + 1));

    // Đọc không xóa message (retention limits): consumer mới thứ hai cũng nhận đủ N.
    const second = await (await openReplay(name, { kind: "all" })).read();
    expect(second.map((m) => m.body)).toEqual(published);
  });

  it("deliver_by_start_time_skips_older_messages", async () => {
    const name = await newStream();
    const older = bodies(5, "old");
    const newer = bodies(5, "new");
    const olderSeqs = await publishBatch(name, older);
    const newerSeqs = await publishBatch(name, newer);

    // Mốc thời gian lấy từ timestamp server gán cho message, không dùng đồng hồ của client:
    // đồng hồ của VM Docker có thể lệch đồng hồ của máy chạy test.
    const lastOlder = await storedMessageTime(name, olderSeqs.at(-1)!);
    const firstNewer = await storedMessageTime(name, newerSeqs[0]!);
    // Điều kiện tiên quyết của test: server gán timestamp tăng thật sự giữa hai batch (độ phân giải nanosecond).
    expect(parseNanos(firstNewer)).toBeGreaterThan(parseNanos(lastOlder));
    const startTime = startTimeBetween(lastOlder, firstNewer);

    // DeliverByStartTime chọn message đầu tiên có timestamp >= startTime, nên chỉ còn batch mới.
    const reader = await openReplay(name, { kind: "by_start_time", startTime });
    expect(reader.pendingAtStart).toBe(newer.length);
    expect((await reader.read()).map((m) => m.body)).toEqual(newer);
  });

  it("by_start_time_after_last_message_starts_at_next_message", async () => {
    const name = await newStream();
    const seqs = await publishBatch(name, bodies(3, "old"));

    // Mốc nằm sau message cuối cùng một giờ (tính từ timestamp của server): không message nào có timestamp >= mốc.
    const lastOlder = await storedMessageTime(name, seqs.at(-1)!);
    const startTime = new Date(
      Number(parseNanos(lastOlder) / 1_000_000n) + 3_600_000,
    ).toISOString();
    const reader = await openReplay(name, { kind: "by_start_time", startTime });
    // Đo trên server 2.15.0: consumer vẫn tạo được, không có gì để giao và vị trí bắt đầu ở sau message cuối.
    expect(reader.pendingAtStart).toBe(0);
    expect(await reader.read()).toEqual([]);

    // Message publish sau đó vẫn được giao dù timestamp của nó nhỏ hơn mốc: consumer bắt đầu ở message kế tiếp (giống `new`).
    const [seq] = await publishBatch(name, ["after"]);
    expect(parseNanos(await storedMessageTime(name, seq!))).toBeLessThan(parseNanos(startTime));
    expect((await reader.read()).map((m) => m.body)).toEqual(["after"]);
  });
});
