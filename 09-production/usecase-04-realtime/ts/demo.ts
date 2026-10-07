// Demo realtime: hai instance server WebSocket dùng chung một Redis (đại diện cho hai máy), ba client,
// một message đi từ instance này sang instance kia qua Redis Pub/Sub, và bảng xếp hạng trên sorted set.
// Mọi channel và key nằm dưới một prefix riêng và bị xóa ở cuối.
import { Redis } from "ioredis";
import WebSocket from "ws";
import { eventually, uniqueName } from "@handbook/testkit";
import { createRealtimeServer, roomChannel, type ChatMessage, type RealtimeServer } from "./lab.js";

const url = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
const prefix = uniqueName("demo-uc04");
const rdb = new Redis(url);

interface Peer {
  label: string;
  ws: WebSocket;
  received: ChatMessage[];
}

/** Escape ký tự glob của MATCH: \ * ? [ ]. */
function escapeGlob(text: string): string {
  return text.replace(/[\\*?[\]]/g, "\\$&");
}

async function numSub(channel: string): Promise<number> {
  const reply = (await rdb.pubsub("NUMSUB", channel)) as [string, number];
  return Number(reply[1]);
}

async function deleteByPrefix(): Promise<void> {
  let cursor = "0";
  do {
    const [next, keys] = await rdb.scan(cursor, "MATCH", `${escapeGlob(prefix)}*`, "COUNT", 200);
    if (keys.length > 0) await rdb.del(...keys);
    cursor = next;
  } while (cursor !== "0");
}

async function until(what: string, fn: () => Promise<boolean | undefined> | boolean | undefined) {
  try {
    await eventually(async () => fn() || undefined, { timeoutMs: 5000, intervalMs: 20 });
  } catch (error) {
    throw new Error(`hết 5000ms khi chờ: ${what}`, { cause: error });
  }
}

const peers: Peer[] = [];

async function join(server: RealtimeServer, label: string, room: string): Promise<Peer> {
  const ws = new WebSocket(server.wsUrl(room, label));
  const peer: Peer = { label, ws, received: [] };
  peers.push(peer);
  ws.on("error", (error) => console.error(`  [${label}] lỗi socket: ${error.message}`));
  ws.on("message", (data) => {
    const message = JSON.parse(data.toString()) as ChatMessage;
    peer.received.push(message);
    console.log(
      `  [${label}] nhận từ ${message.from} trong phòng ${message.room}: "${message.text}"`,
    );
  });
  await new Promise<void>((resolve, reject) => {
    ws.once("open", resolve);
    ws.once("error", reject);
  });
  return peer;
}

async function chatDemo(a: RealtimeServer, b: RealtimeServer): Promise<void> {
  console.log("== 1. Chat: message đi từ instance B sang client của instance A ==");
  const channel = roomChannel(prefix, "general");
  const alice = await join(a, "alice", "general");
  const bob = await join(b, "bob", "general");
  const carol = await join(b, "carol", "random");
  console.log(`alice ở instance A (${a.url}), bob và carol ở instance B (${b.url}).`);
  console.log("alice và bob ở phòng general, carol ở phòng random.");

  // Sẵn sàng một cách xác định: đợi cả hai instance subscribe channel của phòng.
  await until(
    "hai instance subscribe channel của phòng",
    async () => (await numSub(channel)) === 2,
  );
  console.log(
    `PUBSUB NUMSUB ${channel.split(":").slice(1).join(":")} = 2: cả hai instance đã subscribe.`,
  );

  console.log('bob gửi "chào mọi người" qua instance B:');
  bob.ws.send(JSON.stringify({ text: "chào mọi người" }));
  await until(
    "alice và bob nhận message",
    () => alice.received.length >= 1 && bob.received.length >= 1,
  );
  carol.ws.send(JSON.stringify({ text: "tôi ở phòng khác" }));
  await until("carol nhận message của phòng random", () => carol.received.length >= 1);
  console.log(
    `alice nhận ${alice.received.length} bản, bob (người gửi) nhận ${bob.received.length} bản, carol chỉ nhận message của phòng mình (${carol.received.length} bản).`,
  );
  console.log(
    "Người gửi cũng nhận lại message của mình đúng một lần: instance không giao cục bộ, mọi message đều đi qua Redis.\n",
  );
}

async function leaderboardDemo(a: RealtimeServer, b: RealtimeServer): Promise<void> {
  console.log("== 2. Bảng xếp hạng: POST lên instance A, GET top từ instance B ==");
  const post = (server: RealtimeServer, member: string, score: number) =>
    fetch(`${server.url}/leaderboard/season1`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ member, score }),
    });
  const show = async (title: string) => {
    const response = await fetch(`${b.url}/leaderboard/season1/top?n=5`);
    const body = (await response.json()) as { entries: { member: string; score: number }[] };
    console.log(
      `${title}: ${body.entries.map((entry, i) => `${i + 1}. ${entry.member}=${entry.score}`).join("  ")}`,
    );
  };
  for (const [member, score] of [
    ["alice", 120],
    ["bob", 300],
    ["carol", 200],
    ["dave", 200],
  ] as const) {
    await post(a, member, score);
  }
  await show("Top 5 sau 4 lần POST");
  console.log(
    "carol và dave cùng 200 điểm: Redis xếp member theo thứ tự từ điển, ZRANGE REV đảo chiều nên dave đứng trước.",
  );
  await post(a, "alice", 350);
  await show("alice được đặt lại 350 điểm (ZADD đặt score, không cộng dồn)");
  const bad = await fetch(`${b.url}/leaderboard/season1/top?n=0`);
  console.log(`GET top?n=0 trả HTTP ${bad.status}: ${JSON.stringify(await bad.json())}\n`);
}

async function disconnectDemo(a: RealtimeServer): Promise<void> {
  console.log("== 3. Client đứt đột ngột: instance dọn phòng và unsubscribe ==");
  const channel = roomChannel(prefix, "lonely");
  const dave = await join(a, "dave", "lonely");
  await until("instance A subscribe phòng lonely", async () => (await numSub(channel)) === 1);
  console.log(
    `Instance A có ${a.clientCount("lonely")} client trong phòng lonely, NUMSUB = ${await numSub(channel)}.`,
  );
  dave.ws.terminate();
  await until("instance A bỏ client đã đứt", () => a.clientCount("lonely") === 0);
  await until("instance A unsubscribe channel", async () => (await numSub(channel)) === 0);
  console.log(
    `Sau terminate() (không có close frame): clientCount = ${a.clientCount("lonely")}, NUMSUB = ${await numSub(channel)}. Instance đã unsubscribe vì không còn client nào.\n`,
  );
}

const errors: unknown[] = [];
const servers: RealtimeServer[] = [];
try {
  const a = await createRealtimeServer({ redisUrl: url, prefix });
  servers.push(a);
  const b = await createRealtimeServer({ redisUrl: url, prefix });
  servers.push(b);
  await chatDemo(a, b);
  await leaderboardDemo(a, b);
  await disconnectDemo(a);
} catch (error) {
  errors.push(error);
}

// Dọn theo pha tuần tự: đóng client, shutdown instance, xóa key, rồi mới đóng connection của demo.
for (const peer of peers) peer.ws.terminate();
for (const result of await Promise.allSettled(servers.map((server) => server.shutdown()))) {
  if (result.status === "rejected") errors.push(result.reason);
}
try {
  await deleteByPrefix();
  const leftover = await rdb.pubsub("CHANNELS", `${escapeGlob(prefix)}*`);
  if ((leftover as string[]).length > 0)
    errors.push(new Error(`channel còn sót: ${String(leftover)}`));
} catch (error) {
  errors.push(error);
}
await rdb.quit().catch(() => rdb.disconnect());
if (errors.length > 0) {
  console.error("Demo lỗi:", errors[0]);
  process.exitCode = 1;
} else {
  console.log(
    "Đã đóng mọi client và instance, xóa mọi key và không còn subscription nào của demo.",
  );
}
