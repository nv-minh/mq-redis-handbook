// Demo: hai member chia ba partition, một member rời group thì member còn lại nhận hết, rồi bốn member trên ba partition.
import { eventually, uniqueName } from "@handbook/testkit";
import {
  closeProducer,
  createTopic,
  deleteGroup,
  deleteTopic,
  produceToPartition,
  startGroupMember,
  type GroupMember,
} from "./lab.js";

interface Member {
  name: string;
  handle: GroupMember;
  assignment: number[] | undefined;
  received: { partition: number; value: string }[];
}

const topic = uniqueName("demo-group");
const groups: string[] = [];
const started: Member[] = [];

async function join(groupId: string, name: string): Promise<Member> {
  const member: Member = {
    name,
    handle: { stop: async () => undefined },
    assignment: undefined,
    received: [],
  };
  started.push(member);
  member.handle = await startGroupMember(
    topic,
    groupId,
    (partitions) => {
      member.assignment = [...partitions].sort((a, b) => a - b);
      console.log(`  ${name} được gán partition [${member.assignment.join(", ")}]`);
    },
    (m) => member.received.push(m),
  );
  return member;
}

/** Chờ tới khi assignment của các member rời nhau và hợp lại đủ ba partition. */
async function stable(members: Member[]): Promise<void> {
  await eventually(
    async () => {
      if (members.some((m) => m.assignment === undefined)) return undefined;
      const all = members.flatMap((m) => m.assignment ?? []).sort((a, b) => a - b);
      return all.join() === "0,1,2" ? true : undefined;
    },
    { timeoutMs: 45_000 },
  );
}

function describeMembers(members: Member[]): string {
  return members.map((m) => `${m.name}: [${(m.assignment ?? []).join(", ")}]`).join(" | ");
}

try {
  await createTopic(topic, 3);
  console.log(`topic ${topic} có 3 partition`);

  const groupA = uniqueName("demo-group-a");
  groups.push(groupA);
  console.log("--- hai member cùng một group ---");
  const m1 = await join(groupA, "member-1");
  const m2 = await join(groupA, "member-2");
  await stable([m1, m2]);
  console.log(`assignment ổn định: ${describeMembers([m1, m2])}`);
  for (const p of [0, 1, 2]) await produceToPartition(topic, p, `msg-partition-${p}`);
  await eventually(async () => (m1.received.length + m2.received.length >= 3 ? true : undefined), {
    timeoutMs: 30_000,
  });
  for (const m of [m1, m2]) {
    console.log(
      `${m.name} nhận: ${m.received.map((r) => `${r.value}`).join(", ") || "(không có)"}`,
    );
  }
  console.log(
    "Kết luận: mỗi partition chỉ có một chủ, message của partition nào tới đúng member sở hữu partition đó.",
  );

  console.log("--- member-2 rời group (LeaveGroup) ---");
  const left = Date.now();
  await m2.handle.stop();
  await eventually(async () => (m1.assignment?.join() === "0,1,2" ? true : undefined), {
    timeoutMs: 45_000,
  });
  console.log(
    `member-1 giữ cả ba partition sau ${Date.now() - left} ms (một lần đo, không phải cam kết)`,
  );
  console.log(
    "Kết luận: rebalance chuyển partition của member đã rời sang member còn lại. Nếu member chết đột ngột, coordinator phải chờ session.timeout.ms mới phát hiện.",
  );

  const groupB = uniqueName("demo-group-b");
  groups.push(groupB);
  console.log("--- bốn member trên ba partition (group mới) ---");
  const four = await Promise.all(["a", "b", "c", "d"].map((n) => join(groupB, `member-${n}`)));
  await stable(four);
  console.log(`assignment ổn định: ${describeMembers(four)}`);
  console.log(
    "Kết luận: số member nhiều hơn số partition thì member thừa ngồi không, nên số partition là giới hạn của song song hóa.",
  );
} finally {
  await Promise.all(started.map((m) => m.handle.stop()));
  await closeProducer();
  for (const group of groups) await deleteGroup(group);
  await deleteTopic(topic);
}
