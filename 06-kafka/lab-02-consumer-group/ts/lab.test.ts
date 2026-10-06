import { afterEach, describe, expect, it } from "vitest";
import { eventually, uniqueName } from "@handbook/testkit";
import {
  closeProducer,
  createTopic,
  deleteGroup,
  deleteTopic,
  produceToPartition,
  startGroupMember,
} from "./lab.js";

const PARTITIONS = 3;
const ALL = [0, 1, 2];
const topics: string[] = [];
const groups: string[] = [];

/** Một member của group kèm những gì nó đã thấy: assignment mới nhất và các message đã nhận. */
interface Member {
  stop(): Promise<void>;
  /** Assignment của lần onAssign gần nhất, undefined nếu member chưa được gán lần nào. */
  assignment: number[] | undefined;
  received: { partition: number; value: string }[];
}

const members: Member[] = [];

async function newTopic(): Promise<string> {
  const topic = uniqueName("lab02-orders");
  // Đăng ký tên trước khi tạo: xóa một topic chưa tồn tại chỉ là no-op.
  topics.push(topic);
  await createTopic(topic, PARTITIONS);
  return topic;
}

function newGroup(): string {
  const group = uniqueName("lab02-group");
  groups.push(group);
  return group;
}

async function join(topic: string, groupId: string): Promise<Member> {
  const member: Member = { stop: async () => undefined, assignment: undefined, received: [] };
  members.push(member);
  const handle = await startGroupMember(
    topic,
    groupId,
    (partitions) => {
      member.assignment = [...partitions].sort((a, b) => a - b);
    },
    (m) => {
      member.received.push(m);
    },
  );
  member.stop = handle.stop;
  return member;
}

/**
 * Chỉ khi assignment của mọi member đã ổn định mới trả về: mọi member đã được gán ít nhất một lần,
 * các assignment rời nhau và hợp lại đúng ba partition. Trạng thái trung gian của rebalance không bao giờ thỏa điều kiện này.
 */
async function stableAssignments(group: Member[]): Promise<number[][]> {
  return eventually(
    async () => {
      const assignments = group.map((m) => m.assignment);
      if (assignments.some((a) => a === undefined)) return undefined;
      const all = assignments.flatMap((a) => a ?? []);
      const covers = [...all].sort((a, b) => a - b).join() === ALL.join();
      return covers ? assignments.map((a) => a ?? []) : undefined;
    },
    { timeoutMs: 45_000 },
  );
}

afterEach(async () => {
  await Promise.all(members.splice(0).map((m) => m.stop().catch(() => undefined)));
  await closeProducer();
  for (const group of groups.splice(0)) await deleteGroup(group);
  for (const topic of topics.splice(0)) await deleteTopic(topic);
});

describe("lab-02 consumer group: chia partition giữa các member", () => {
  it("partitions_are_split_across_group_members", async () => {
    const topic = await newTopic();
    const groupId = newGroup();
    const m1 = await join(topic, groupId);
    const m2 = await join(topic, groupId);

    const [a1, a2] = await stableAssignments([m1, m2]);
    // Assignment rời nhau và phủ đủ ba partition (stableAssignments đã kiểm tra phần phủ, ở đây kiểm tra không chồng).
    expect(a1?.filter((p) => a2?.includes(p))).toEqual([]);
    expect([...(a1 ?? []), ...(a2 ?? [])].sort((a, b) => a - b)).toEqual(ALL);
    expect(a1?.length).toBeGreaterThan(0);
    expect(a2?.length).toBeGreaterThan(0);

    // Mỗi partition nhận một message, và message chỉ tới member đang sở hữu partition đó.
    for (const partition of ALL) await produceToPartition(topic, partition, `p${partition}`);
    await eventually(
      async () => (m1.received.length + m2.received.length >= PARTITIONS ? true : undefined),
      { timeoutMs: 30_000 },
    );
    for (const m of [m1, m2]) {
      for (const r of m.received) expect(m.assignment).toContain(r.partition);
    }
    expect([...m1.received, ...m2.received].map((r) => r.value).sort()).toEqual(["p0", "p1", "p2"]);
  }, 60_000);

  it("rebalance_reassigns_partitions_when_member_leaves", async () => {
    const topic = await newTopic();
    const groupId = newGroup();
    const m1 = await join(topic, groupId);
    const m2 = await join(topic, groupId);
    await stableAssignments([m1, m2]);

    // Rời group một cách có chủ đích (LeaveGroup) để rebalance chạy ngay, không chờ session.timeout.ms (30-45 giây).
    // Member chết đột ngột thì coordinator chỉ phát hiện sau tối đa session.timeout.ms.
    await m2.stop();

    // Chỉ assert trạng thái ổn định cuối cùng: member còn lại giữ cả ba partition.
    await eventually(async () => (m1.assignment?.join() === ALL.join() ? true : undefined), {
      timeoutMs: 45_000,
    });
    expect(m1.assignment).toEqual(ALL);

    // Member còn lại thật sự nhận message của mọi partition, kể cả partition vừa nhận từ member đã rời.
    for (const partition of ALL) await produceToPartition(topic, partition, `after-${partition}`);
    await eventually(
      async () => {
        const values = new Set(m1.received.map((r) => r.value));
        return ALL.every((p) => values.has(`after-${p}`)) ? true : undefined;
      },
      { timeoutMs: 30_000 },
    );
  }, 90_000);

  it("members_beyond_partition_count_stay_idle", async () => {
    const topic = await newTopic();
    const groupId = newGroup();
    const four = await Promise.all([1, 2, 3, 4].map(() => join(topic, groupId)));

    const assignments = await stableAssignments(four);
    // Ba partition chia cho bốn member: ba member mỗi người một partition, đúng một member không có gì.
    expect(assignments.map((a) => a.length).sort((x, y) => x - y)).toEqual([0, 1, 1, 1]);
    const idle = four.find((m) => m.assignment?.length === 0);
    expect(idle).toBeDefined();

    for (const partition of ALL) await produceToPartition(topic, partition, `p${partition}`);
    await eventually(
      async () =>
        four.reduce((sum, m) => sum + m.received.length, 0) >= PARTITIONS ? true : undefined,
      { timeoutMs: 30_000 },
    );
    // Cả ba message đã tới ba member có partition, nên member thừa chắc chắn không nhận gì.
    expect(idle?.received).toEqual([]);
    for (const m of four) {
      for (const r of m.received) expect(m.assignment).toContain(r.partition);
    }
  }, 90_000);
});
