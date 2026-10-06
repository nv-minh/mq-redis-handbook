import { afterEach, describe, expect, it } from "vitest";
import { uniqueName } from "@handbook/testkit";
import {
  closeProducer,
  consumeOneThenCrash,
  createTopic,
  deleteGroup,
  deleteTopic,
  produce,
  receiveUntil,
  replayFromBeginning,
} from "./lab.js";

const PARTITIONS = 3;
const topics: string[] = [];
const groups: string[] = [];

async function newTopic(): Promise<string> {
  const topic = uniqueName("lab03-events");
  // Đăng ký tên trước khi tạo: xóa một topic chưa tồn tại chỉ là no-op.
  topics.push(topic);
  await createTopic(topic, PARTITIONS);
  return topic;
}

/** Mỗi test và mỗi lần replay dùng group id mới, để offset đã commit của lần trước không ảnh hưởng. */
function newGroup(): string {
  const group = uniqueName("lab03-group");
  groups.push(group);
  return group;
}

afterEach(async () => {
  await closeProducer();
  for (const group of groups.splice(0)) await deleteGroup(group);
  for (const topic of topics.splice(0)) await deleteTopic(topic);
});

describe("lab-03 offsets và replay", () => {
  it("new_group_with_earliest_replays_all_messages", async () => {
    const topic = await newTopic();
    const expected: string[] = [];
    // Sáu key khác nhau để message nằm trên nhiều partition.
    for (let i = 0; i < 12; i++) {
      const value = `event-${i}`;
      await produce(topic, `key-${i % 6}`, value);
      expected.push(value);
    }

    // Group mới chưa có offset đã commit nên auto.offset.reset=earliest cho đọc lại toàn bộ log.
    const first = await replayFromBeginning(topic, newGroup());
    expect([...first].sort()).toEqual([...expected].sort());

    // Kafka không xóa message sau khi đọc: group mới thứ hai cũng nhận đủ.
    const second = await replayFromBeginning(topic, newGroup());
    expect([...second].sort()).toEqual([...expected].sort());
  }, 60_000);

  it("replay_of_an_empty_topic_returns_an_empty_list", async () => {
    const topic = await newTopic();
    // Topic rỗng: điều kiện dừng (tổng high watermark trừ low watermark bằng 0) thỏa ngay, không chờ message nào.
    expect(await replayFromBeginning(topic, newGroup())).toEqual([]);
  }, 60_000);

  it("commit_after_processing_loses_nothing_on_crash", async () => {
    const topic = await newTopic();
    const groupId = newGroup();
    await produce(topic, "order-1", "M1");

    // Xử lý M1 rồi "crash" TRƯỚC khi commit: offset của group không nhúc nhích.
    const processed: string[] = [];
    await consumeOneThenCrash(topic, groupId, "process-then-commit", (value) =>
      processed.push(value),
    );
    expect(processed).toEqual(["M1"]);

    // M2 cùng key nên cùng partition với M1, và Kafka giữ thứ tự trong một partition.
    await produce(topic, "order-1", "M2");

    // Consumer mới trong cùng group đọc lại từ offset đã commit, tức là từ M1: at-least-once, M1 được xử lý lần hai.
    expect(await receiveUntil(topic, groupId, "M2")).toEqual(["M1", "M2"]);
  }, 60_000);

  it("commit_before_processing_loses_message_on_crash", async () => {
    const topic = await newTopic();
    const groupId = newGroup();
    await produce(topic, "order-1", "M1");

    // Commit M1 rồi "crash" TRƯỚC khi xử lý: offset đã đi qua M1 nhưng M1 chưa từng được xử lý.
    const processed: string[] = [];
    await consumeOneThenCrash(topic, groupId, "commit-then-process", (value) =>
      processed.push(value),
    );
    expect(processed).toEqual([]);

    await produce(topic, "order-1", "M2");

    // Consumer mới bắt đầu sau offset đã commit nên không bao giờ thấy M1: at-most-once, M1 mất.
    // M2 cùng partition và đến sau M1, nên khi đã nhận M2 thì chắc chắn M1 sẽ không còn tới nữa.
    expect(await receiveUntil(topic, groupId, "M2")).toEqual(["M2"]);
  }, 60_000);
});
