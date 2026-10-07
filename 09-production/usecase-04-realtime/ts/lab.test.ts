import { randomBytes } from "node:crypto";
import net from "node:net";
import { afterAll, afterEach, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import WebSocket from "ws";
import { eventually, uniqueName } from "@handbook/testkit";
import {
  createRealtimeServer,
  roomChannel,
  type ChatMessage,
  type RealtimeOptions,
  type RealtimeServer,
} from "./lab.js";

const url = process.env.REDIS_URL ?? "redis://127.0.0.1:6379";
// Connection của test, dùng cho PUBLISH, PUBSUB NUMSUB và SCAN. Không bao giờ subscribe.
const rdb = new Redis(url);

interface TestClient {
  ws: WebSocket;
  received: ChatMessage[];
  errors: Error[];
  /** Resolve khi socket đóng, kèm close code (1006 nếu đứt mà không có close frame). */
  closed: Promise<{ code: number; reason: string }>;
  /** Giống `closed` nhưng đọc đồng bộ: undefined khi socket chưa đóng. */
  closeInfo?: { code: number; reason: string };
}

// Mọi tài nguyên của test được ghi lại ở đây để afterEach dọn dù test fail giữa chừng.
const prefixes: string[] = [];
const servers: RealtimeServer[] = [];
const clients: TestClient[] = [];
const rawSockets: net.Socket[] = [];

function newPrefix(label: string): string {
  const prefix = uniqueName(`uc04-${label}`);
  prefixes.push(prefix);
  return prefix;
}

async function startServer(
  prefix: string,
  overrides: Partial<RealtimeOptions> = {},
): Promise<RealtimeServer> {
  const server = await createRealtimeServer({ redisUrl: url, prefix, ...overrides });
  servers.push(server);
  return server;
}

/** Chờ điều kiện với timeout ngắn hơn timeout của test, và message nói rõ đang chờ gì. */
async function waitUntil<T>(
  what: string,
  fn: () => Promise<T | undefined | false> | T | undefined | false,
  timeoutMs = 5000,
): Promise<T> {
  try {
    return await eventually(async () => fn(), { timeoutMs, intervalMs: 20 });
  } catch (error) {
    throw new Error(`hết ${timeoutMs}ms khi chờ: ${what}`, { cause: error });
  }
}

/** Mở một WebSocket client và chờ tới khi nó open. Mọi socket đều có listener 'error'. */
async function connect(
  server: RealtimeServer,
  room: string,
  options: { name?: string; origin?: string } = {},
): Promise<TestClient> {
  const ws = new WebSocket(server.wsUrl(room, options.name), { origin: options.origin });
  const client: TestClient = {
    ws,
    received: [],
    errors: [],
    closed: new Promise((resolve) =>
      ws.once("close", (code, reason) => {
        client.closeInfo = { code, reason: reason.toString() };
        resolve(client.closeInfo);
      }),
    ),
  };
  ws.on("error", (error) => client.errors.push(error));
  ws.on("message", (data) => client.received.push(JSON.parse(data.toString()) as ChatMessage));
  clients.push(client);
  await new Promise<void>((resolve, reject) => {
    ws.once("open", resolve);
    ws.once("error", reject);
    ws.once("close", () => reject(new Error("socket đóng trước khi open")));
  });
  return client;
}

/** Thử upgrade và trả về status HTTP của lần từ chối (hoặc 101 nếu được nhận). */
async function upgradeStatus(
  wsUrl: string,
  origin?: string,
): Promise<{ status: number; body: string }> {
  const ws = new WebSocket(wsUrl, { origin });
  ws.on("error", () => undefined);
  try {
    return await new Promise((resolve, reject) => {
      ws.once("open", () => resolve({ status: 101, body: "" }));
      ws.once("unexpected-response", (_request, response) => {
        let body = "";
        response.on("data", (chunk: Buffer) => (body += chunk.toString()));
        response.on("end", () => resolve({ status: response.statusCode ?? 0, body }));
        response.on("error", reject);
      });
      ws.once("error", reject);
    });
  } finally {
    ws.terminate();
  }
}

/** Chờ socket đóng, fail nhanh với message rõ ràng thay vì treo tới hết timeout của test. */
function waitClosed(client: TestClient, what: string): Promise<{ code: number; reason: string }> {
  return waitUntil(what, () => client.closeInfo);
}

function send(client: TestClient, text: string): void {
  client.ws.send(JSON.stringify({ text }));
}

function waitForMessages(client: TestClient, count: number, what: string): Promise<void> {
  return waitUntil(what, () => client.received.length >= count || undefined).then(() => undefined);
}

async function numSub(channel: string): Promise<number> {
  const reply = (await rdb.pubsub("NUMSUB", channel)) as [string, number];
  return Number(reply[1]);
}

/** Escape ký tự glob của MATCH: \ * ? [ ]. */
function escapeGlob(text: string): string {
  return text.replace(/[\\*?[\]]/g, "\\$&");
}

async function scanKeys(prefix: string): Promise<string[]> {
  const found: string[] = [];
  let cursor = "0";
  do {
    const [next, keys] = await rdb.scan(cursor, "MATCH", `${escapeGlob(prefix)}*`, "COUNT", 200);
    found.push(...keys);
    cursor = next;
  } while (cursor !== "0");
  return found;
}

async function leftoverChannels(prefix: string): Promise<string[]> {
  return (await rdb.pubsub("CHANNELS", `${escapeGlob(prefix)}*`)) as string[];
}

/**
 * Chạy các pha TUẦN TỰ: pha sau chỉ bắt đầu khi pha trước đã xong hẳn. Một pha lỗi không làm bỏ qua
 * các pha sau (việc dọn vẫn phải chạy tiếp), và lỗi đầu tiên được ném lại ở cuối.
 * Trong một pha, các việc độc lập chạy song song (allSettled) và lỗi của chúng cũng được gom lại.
 */
async function runPhases(phases: (() => Promise<unknown>)[]): Promise<void> {
  const errors: unknown[] = [];
  for (const phase of phases) {
    try {
      await phase();
    } catch (error) {
      errors.push(error);
    }
  }
  if (errors.length > 0) throw errors[0];
}

async function settleAll(tasks: Promise<unknown>[]): Promise<void> {
  const results = await Promise.allSettled(tasks);
  const failed = results.find((result) => result.status === "rejected");
  if (failed) throw (failed as PromiseRejectedResult).reason;
}

afterEach(async () => {
  const ownClients = clients.splice(0);
  const ownSockets = rawSockets.splice(0);
  const ownServers = servers.splice(0);
  const ownPrefixes = prefixes.splice(0);
  await runPhases([
    // Pha 1: đóng mọi client (terminate không cần handshake) và chờ socket thật sự đóng.
    async () => {
      for (const socket of ownSockets) socket.destroy();
      for (const client of ownClients) client.ws.terminate();
      await settleAll(ownClients.map((client) => client.closed));
    },
    // Pha 2: shutdown mọi instance (idempotent, nên instance đã tự shutdown trong test cũng ổn).
    () => settleAll(ownServers.map((server) => server.shutdown())),
    // Pha 3: xóa key của test bằng SCAN theo prefix (không bao giờ KEYS trên Redis dùng chung).
    async () => {
      for (const prefix of ownPrefixes) {
        const keys = await scanKeys(prefix);
        if (keys.length > 0) await rdb.del(...keys);
      }
    },
    // Pha 4: không còn key hay subscription nào của test sót lại.
    async () => {
      for (const prefix of ownPrefixes) {
        expect(await scanKeys(prefix), `key còn sót của prefix ${prefix}`).toEqual([]);
        expect(await leftoverChannels(prefix), `channel còn sót của prefix ${prefix}`).toEqual([]);
      }
    },
  ]);
});

afterAll(async () => {
  await rdb.quit().catch(() => rdb.disconnect());
});

describe("usecase-04 realtime: fan-out WebSocket qua Redis Pub/Sub", () => {
  it("client_on_instance_a_receives_message_sent_via_instance_b", async () => {
    const prefix = newPrefix("fanout");
    const instanceA = await startServer(prefix);
    const instanceB = await startServer(prefix);
    const room = "general";
    const otherRoom = "random";

    const alice = await connect(instanceA, room, { name: "alice" });
    const bob = await connect(instanceB, room, { name: "bob" });
    // Client của phòng khác nằm trên instance B, tức dùng CHUNG subscriber connection với bob.
    // Nhờ vậy thứ tự đến của hai channel trên connection đó chứng minh được việc không nhận nhầm.
    const carol = await connect(instanceB, otherRoom, { name: "carol" });

    // Sẵn sàng một cách xác định: cả hai instance đã subscribe channel của phòng (PUBSUB NUMSUB = 2).
    await waitUntil("cả hai instance subscribe channel của phòng", async () => {
      return (await numSub(roomChannel(prefix, room))) === 2 || undefined;
    });
    expect(instanceA.clientCount(room)).toBe(1);
    expect(instanceB.clientCount(room)).toBe(1);

    const before = Date.now();
    send(bob, "hello from B");
    await waitForMessages(alice, 1, "alice nhận message đầu tiên");
    await waitForMessages(bob, 1, "bob nhận lại message của chính mình");

    expect(alice.received[0]).toEqual({
      room,
      from: "bob",
      text: "hello from B",
      ts: expect.any(Number),
    });
    expect(alice.received[0]!.ts).toBeGreaterThanOrEqual(before);
    expect(alice.received[0]!.ts).toBeLessThanOrEqual(Date.now());
    // Người gửi cũng nhận message của mình đúng một lần (quyết định: mọi thứ đi qua Redis).
    expect(bob.received[0]).toEqual(alice.received[0]);

    // Phòng khác gửi message SAU. Khi carol nhận được nó, message trước đã được xử lý xong trên cùng
    // subscriber connection, nên nếu nó bị giao nhầm thì đã nằm trong carol.received rồi.
    send(carol, "only for random");
    await waitForMessages(carol, 1, "carol nhận message của phòng random");
    expect(carol.received.map((message) => message.text)).toEqual(["only for random"]);
    expect(carol.received[0]!.room).toBe(otherRoom);

    // Marker cuối: alice và bob nhận [message 1, marker], không có bản sao thừa chen giữa
    // (một lần giao trùng sẽ đến trước marker vì cùng đi trên một pipeline).
    send(bob, "marker");
    await waitForMessages(alice, 2, "alice nhận marker");
    await waitForMessages(bob, 2, "bob nhận marker");
    expect(alice.received.map((message) => message.text)).toEqual(["hello from B", "marker"]);
    expect(bob.received.map((message) => message.text)).toEqual(["hello from B", "marker"]);
    for (const client of [alice, bob, carol]) expect(client.errors).toEqual([]);
  });

  it("leaderboard_top_returns_scores_in_descending_order", async () => {
    const prefix = newPrefix("lb");
    const instanceA = await startServer(prefix);
    const instanceB = await startServer(prefix);

    const post = (server: RealtimeServer, name: string, body: unknown) =>
      fetch(`${server.url}/leaderboard/${name}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: typeof body === "string" ? body : JSON.stringify(body),
      });
    const top = async (server: RealtimeServer, name: string, query = "") => {
      const response = await fetch(`${server.url}/leaderboard/${name}/top${query}`);
      return { status: response.status, body: (await response.json()) as Record<string, unknown> };
    };
    type Entry = { member: string; score: number };

    // Bảng rỗng trả danh sách rỗng, không phải lỗi.
    const empty = await top(instanceB, "season1", "?n=5");
    expect(empty).toEqual({ status: 200, body: { name: "season1", entries: [] } });

    // POST lên instance A, GET từ instance B: dữ liệu nằm ở Redis dùng chung.
    for (const [member, score] of [
      ["alice", 10],
      ["bob", 30],
      ["carol", 20],
      ["dave", 20],
      ["eve", 2.5],
    ] as const) {
      const response = await post(instanceA, "season1", { member, score });
      expect(response.status).toBe(200);
      expect(await response.json()).toEqual({ name: "season1", member, score });
    }

    const all = await top(instanceB, "season1", "?n=100");
    expect(all.status).toBe(200);
    // Giảm dần theo score. Hai member cùng score 20 theo thứ tự GIẢM dần của member (dave trước carol):
    // Redis sắp xếp score bằng nhau theo thứ tự từ điển của member, và ZRANGE ... REV đảo cả hai chiều.
    expect(all.body.entries).toEqual([
      { member: "bob", score: 30 },
      { member: "dave", score: 20 },
      { member: "carol", score: 20 },
      { member: "alice", score: 10 },
      { member: "eve", score: 2.5 },
    ]);
    // Score là number trong JSON (RESP3 trả double, không phải chuỗi).
    for (const entry of all.body.entries as Entry[]) expect(typeof entry.score).toBe("number");

    // n nhỏ hơn kích thước cắt đúng; n lớn hơn kích thước trả đủ; không có n thì mặc định 10.
    expect((await top(instanceB, "season1", "?n=2")).body.entries).toEqual([
      { member: "bob", score: 30 },
      { member: "dave", score: 20 },
    ]);
    expect(((await top(instanceB, "season1", "?n=1000")).body.entries as Entry[]).length).toBe(5);
    expect(((await top(instanceB, "season1")).body.entries as Entry[]).length).toBe(5);

    // n không hợp lệ: 400 kèm message, không bao giờ 500.
    for (const query of ["?n=0", "?n=-3", "?n=abc", "?n=1.5", "?n=", "?n=1001", "?n=1e2"]) {
      const result = await top(instanceB, "season1", query);
      expect(result.status, `GET top ${query}`).toBe(400);
      expect(typeof result.body.error).toBe("string");
    }

    // POST là "đặt score" (ZADD), không phải cộng dồn: cập nhật alice từ 10 lên 40 đưa nó lên đầu,
    // và POST lần nữa với 15 đặt lại đúng 15 (nếu là ZINCRBY thì sẽ ra 55).
    expect((await post(instanceB, "season1", { member: "alice", score: 40 })).status).toBe(200);
    expect(((await top(instanceA, "season1", "?n=2")).body.entries as Entry[])[0]).toEqual({
      member: "alice",
      score: 40,
    });
    await post(instanceA, "season1", { member: "alice", score: 15 });
    const afterReset = (await top(instanceB, "season1", "?n=100")).body.entries as Entry[];
    expect(afterReset.find((entry) => entry.member === "alice")?.score).toBe(15);
    expect(afterReset.map((entry) => entry.member)).toEqual([
      "bob",
      "dave",
      "carol",
      "alice",
      "eve",
    ]);

    // Các bảng độc lập với nhau.
    expect((await top(instanceB, "season2", "?n=5")).body.entries).toEqual([]);

    // Body hoặc tên sai: 400, và không ghi gì vào Redis.
    const badBodies: unknown[] = [
      "not json",
      "[]",
      "null",
      { score: 1 },
      { member: "", score: 1 },
      { member: 7, score: 1 },
      { member: "x", score: "5" },
      { member: "x" },
      '{"member":"x","score":1e999}',
      { member: "m".repeat(129), score: 1 },
    ];
    for (const body of badBodies) {
      const response = await post(instanceA, "season2", body);
      expect(response.status, `POST ${JSON.stringify(body)}`).toBe(400);
    }
    expect((await post(instanceA, "bad%20name", { member: "x", score: 1 })).status).toBe(400);
    expect((await top(instanceB, "bad%20name", "?n=5")).status).toBe(400);
    expect((await top(instanceB, "season2", "?n=5")).body.entries).toEqual([]);
    expect(await rdb.exists(`${prefix}:lb:season2`)).toBe(0);
  });

  it("disconnected_client_is_removed_from_room", async () => {
    const prefix = newPrefix("leave");
    const instanceA = await startServer(prefix);
    const instanceB = await startServer(prefix);
    const room = "lobby";
    const channel = roomChannel(prefix, room);

    expect(instanceA.roomCount()).toBe(0);
    const graceful = await connect(instanceA, room, { name: "graceful" });
    const abrupt = await connect(instanceA, room, { name: "abrupt" });
    const stayer = await connect(instanceB, room, { name: "stayer" });
    await waitUntil("hai instance subscribe channel", async () => {
      return (await numSub(channel)) === 2 || undefined;
    });
    expect(instanceA.clientCount(room)).toBe(2);
    expect(instanceB.clientCount(room)).toBe(1);

    // Đóng lịch sự: có close handshake. Instance A bỏ client khỏi phòng nhưng còn abrupt nên vẫn subscribe.
    graceful.ws.close(1000, "bye");
    expect((await waitClosed(graceful, "client đóng lịch sự")).code).toBe(1000);
    await waitUntil("instance A bỏ client đóng lịch sự", () => {
      return instanceA.clientCount(room) === 1 || undefined;
    });
    expect(await numSub(channel)).toBe(2);

    // Message tới phòng trong lúc có client đã rời: người còn lại nhận đúng một lần, không lỗi.
    send(stayer, "after graceful leave");
    await waitForMessages(abrupt, 1, "client abrupt nhận message");
    await waitForMessages(stayer, 1, "stayer nhận message của chính mình");
    expect(graceful.received).toEqual([]);

    // Đứt đột ngột: terminate() hủy TCP socket mà không gửi close frame. Instance phải tự phát hiện.
    abrupt.ws.terminate();
    expect((await waitClosed(abrupt, "client bị terminate")).code).toBe(1006);
    await waitUntil("instance A phát hiện socket bị hủy và bỏ client", () => {
      return instanceA.clientCount(room) === 0 || undefined;
    });
    // Client cuối của instance A đã rời: nó unsubscribe, chỉ còn instance B.
    await waitUntil("instance A unsubscribe channel (NUMSUB về 1)", async () => {
      return (await numSub(channel)) === 1 || undefined;
    });
    expect(instanceA.roomCount()).toBe(0);

    // Phòng còn instance B: message vẫn đi qua Redis tới đúng người, instance A không ném lỗi.
    send(stayer, "after abrupt leave");
    await waitForMessages(stayer, 2, "stayer nhận message thứ hai");
    expect(stayer.received.map((message) => message.text)).toEqual([
      "after graceful leave",
      "after abrupt leave",
    ]);

    // Client cuối của instance B rời: NUMSUB về 0, không còn phòng nào trên cả hai instance.
    stayer.ws.close(1000, "bye");
    await waitClosed(stayer, "stayer đóng");
    await waitUntil("instance B unsubscribe channel (NUMSUB về 0)", async () => {
      return (await numSub(channel)) === 0 || undefined;
    });
    expect(instanceB.clientCount(room)).toBe(0);
    expect(instanceB.roomCount()).toBe(0);

    // Publish tới phòng không còn ai: 0 receiver, không ném lỗi, không có gì bị giữ lại.
    expect(await rdb.publish(channel, JSON.stringify({ text: "nobody home" }))).toBe(0);
    // Phòng có thể dùng lại: client mới subscribe lại và nhận message.
    const returning = await connect(instanceA, room, { name: "returning" });
    await waitUntil("instance A subscribe lại", async () => {
      return (await numSub(channel)) === 1 || undefined;
    });
    send(returning, "welcome back");
    await waitForMessages(returning, 1, "client quay lại nhận message");
    for (const client of [graceful, stayer, returning]) expect(client.errors).toEqual([]);
  });
});

describe("usecase-04 realtime: các cạnh của server", () => {
  it("invalid_room_is_rejected_with_400_before_the_upgrade", async () => {
    const prefix = newPrefix("room");
    const server = await startServer(prefix);
    const base = server.wsUrl("x").replace(/\?.*$/, "");
    const cases: [string, string, string][] = [
      ["thiếu room", base, "room"],
      ["room rỗng", `${base}?room=`, "room"],
      ["room quá dài (65 ký tự)", `${base}?room=${"r".repeat(65)}`, "room"],
      ["room có ký tự không cho phép", `${base}?room=a%20b`, "room"],
      ["room có glob", `${base}?room=a*`, "room"],
      ["name quá dài", `${base}?room=ok&name=${"n".repeat(65)}`, "name"],
    ];
    for (const [label, wsUrl, field] of cases) {
      const result = await upgradeStatus(wsUrl);
      expect(result.status, label).toBe(400);
      expect(result.body, label).toContain(field);
    }
    // Room dài đúng 64 ký tự thì được.
    expect((await upgradeStatus(`${base}?room=${"r".repeat(64)}`)).status).toBe(101);
    // Đường dẫn khác /ws không phải endpoint WebSocket.
    expect((await upgradeStatus(base.replace("/ws", "/nope") + "?room=ok")).status).toBe(404);
    // Chưa phòng nào được tạo bởi các request bị từ chối (phòng 64 ký tự đã được dọn).
    await waitUntil("server không còn phòng nào", () => server.roomCount() === 0 || undefined);
  });

  it("cross_origin_upgrade_is_rejected_but_same_origin_and_no_origin_are_allowed", async () => {
    const prefix = newPrefix("origin");
    const server = await startServer(prefix);
    const wsUrl = server.wsUrl("room-o");
    // Lab chạy trên localhost: client không gửi Origin (không phải trình duyệt) và origin cùng host được nhận,
    // origin của trang web khác bị từ chối bằng 403.
    expect((await upgradeStatus(wsUrl)).status).toBe(101);
    expect((await upgradeStatus(wsUrl, server.url)).status).toBe(101);
    expect((await upgradeStatus(wsUrl, "http://evil.example")).status).toBe(403);
    await waitUntil("server không còn phòng nào", () => server.roomCount() === 0 || undefined);
  });

  it("malformed_message_closes_only_that_connection_with_1003", async () => {
    const prefix = newPrefix("malformed");
    const server = await startServer(prefix);
    const room = "bad-input";
    const healthy = await connect(server, room, { name: "healthy" });

    const attempts: [string, (ws: WebSocket) => void][] = [
      ["không phải JSON", (ws) => ws.send("this is not json")],
      ["JSON nhưng không phải object", (ws) => ws.send("[1,2,3]")],
      ["thiếu text", (ws) => ws.send(JSON.stringify({ message: "hi" }))],
      ["text không phải chuỗi", (ws) => ws.send(JSON.stringify({ text: 42 }))],
      ["text rỗng", (ws) => ws.send(JSON.stringify({ text: "" }))],
      ["frame nhị phân", (ws) => ws.send(Buffer.from([1, 2, 3]))],
    ];
    for (const [label, action] of attempts) {
      const offender = await connect(server, room, { name: "offender" });
      action(offender.ws);
      const closed = await waitUntil(`${label}: server đóng connection`, () => offender.closeInfo);
      expect(closed.code, label).toBe(1003);
      expect(closed.reason, label).toContain("JSON");
    }

    // Instance vẫn sống và client khỏe vẫn nhận được message, không nhận message lỗi nào.
    send(healthy, "still alive");
    await waitForMessages(healthy, 1, "client khỏe nhận message");
    expect(healthy.received.map((message) => message.text)).toEqual(["still alive"]);
    await waitUntil("chỉ còn client khỏe trong phòng", () => {
      return server.clientCount(room) === 1 || undefined;
    });
  });

  it("oversized_message_closes_connection_with_1009_and_instance_survives", async () => {
    const prefix = newPrefix("big");
    const server = await startServer(prefix, { maxMessageBytes: 1024 });
    const room = "size-limit";
    const healthy = await connect(server, room, { name: "healthy" });
    const sender = await connect(server, room, { name: "sender" });

    // Dưới giới hạn thì qua (cả frame JSON dưới 1024 byte).
    send(sender, "x".repeat(900));
    await waitForMessages(healthy, 1, "message dưới giới hạn được giao");

    // Vượt giới hạn: thư viện đóng connection với 1009 (Message Too Big) trước khi gom hết payload.
    send(sender, "y".repeat(5000));
    expect((await waitClosed(sender, "server đóng client gửi message quá lớn")).code).toBe(1009);

    send(healthy, "after the big one");
    await waitForMessages(healthy, 2, "client khỏe nhận message sau đó");
    // Message quá lớn không bao giờ tới ai: chỉ có hai message hợp lệ.
    expect(healthy.received.map((message) => message.text)).toEqual([
      "x".repeat(900),
      "after the big one",
    ]);
  });

  it("slow_client_is_dropped_without_blocking_others", async () => {
    const prefix = newPrefix("slow");
    // Message 30 KB và hàng đợi gửi tối đa 512 KiB mỗi client.
    const server = await startServer(prefix, {
      maxMessageBytes: 32_768,
      maxBufferedBytes: 524_288,
    });
    const room = "slow-room";
    const channel = roomChannel(prefix, room);

    // Client "kẹt": hoàn tất handshake rồi KHÔNG BAO GIỜ đọc nữa (socket bị pause).
    const stuck = await connectStuck(server, room);
    let stuckClosed = false;
    stuck.once("close", () => (stuckClosed = true));
    const fast = await connect(server, room, { name: "fast" });
    await waitUntil("instance subscribe channel", async () => {
      return (await numSub(channel)) === 1 || undefined;
    });
    expect(server.clientCount(room)).toBe(2);

    // Gửi từng đợt nhỏ và chờ client nhanh nhận hết đợt đó trước khi gửi tiếp, nên client nhanh không bao giờ
    // tồn đọng nhiều hơn một đợt. Client kẹt thì không đọc, buffer của nó chỉ tăng. Vòng lặp dừng khi server bỏ nó
    // (có chặn trên để test không treo vô hạn).
    const text = "z".repeat(30_000);
    const burst = 5;
    let sent = 0;
    for (let round = 0; server.clientCount(room) === 2; round++) {
      if (round >= 600) throw new Error("server không bỏ client kẹt sau 600 đợt (90 MB)");
      for (let i = 0; i < burst; i++) send(fast, text);
      sent += burst;
      await waitForMessages(fast, sent, `client nhanh nhận đủ ${sent} message`);
    }
    expect(server.clientCount(room)).toBe(1);
    // Server đã hủy TCP connection của client kẹt: đọc nốt dữ liệu còn trong buffer rồi socket phải đóng.
    stuck.resume();
    await waitUntil("socket của client kẹt bị server đóng", () => stuckClosed || undefined);

    // Client nhanh nhận đủ và đúng thứ tự, vẫn kết nối, và vẫn gửi nhận được sau khi client kẹt bị bỏ.
    expect(fast.received).toHaveLength(sent);
    send(fast, "after drop");
    await waitForMessages(fast, sent + 1, "client nhanh nhận message sau khi bỏ client kẹt");
    expect(fast.received.at(-1)!.text).toBe("after drop");
    expect(fast.errors).toEqual([]);
  });

  it("unresponsive_client_is_dropped_by_heartbeat", async () => {
    const prefix = newPrefix("hb");
    const server = await startServer(prefix, { pingIntervalMs: 50 });
    const room = "heartbeat";
    // Client kẹt không bao giờ trả pong. Client khỏe (ws tự trả pong) thì ở lại.
    await connectStuck(server, room);
    const healthy = await connect(server, room, { name: "healthy" });
    expect(server.clientCount(room)).toBe(2);
    await waitUntil("heartbeat bỏ client không trả pong", () => {
      return server.clientCount(room) === 1 || undefined;
    });
    send(healthy, "still here");
    await waitForMessages(healthy, 1, "client khỏe vẫn nhận message");
  });

  it("shutdown_closes_everything_and_is_idempotent", async () => {
    const prefix = newPrefix("shutdown");
    const server = await startServer(prefix);
    const room = "closing";
    const channel = roomChannel(prefix, room);
    const one = await connect(server, room, { name: "one" });
    const two = await connect(server, "closing-2", { name: "two" });
    await waitUntil("instance subscribe hai channel", async () => {
      return (await numSub(channel)) === 1 || undefined;
    });
    expect(server.redisStatus()).toEqual({ command: "ready", subscriber: "ready" });

    // Gọi đồng thời hai lần và gọi lại sau khi xong: mọi lần cùng resolve, không ném lỗi.
    await Promise.all([server.shutdown(), server.shutdown()]);
    await server.shutdown();

    expect((await waitClosed(one, "client one nhận close frame")).code).toBe(1001);
    expect((await waitClosed(two, "client two nhận close frame")).code).toBe(1001);
    expect(server.clientCount(room)).toBe(0);
    expect(server.roomCount()).toBe(0);
    expect(server.redisStatus()).toEqual({ command: "end", subscriber: "end" });
    // Redis dọn subscription của connection đã đóng bất đồng bộ (khi đọc tới EOF), nên dùng waitUntil.
    await waitUntil("NUMSUB về 0 sau shutdown", async () => {
      return (await numSub(channel)) === 0 || undefined;
    });
    // HTTP server đã dừng: không nhận kết nối mới.
    await expect(fetch(`${server.url}/leaderboard/x/top?n=1`)).rejects.toThrow();
    await expect(upgradeStatus(server.wsUrl(room))).rejects.toThrow();
  });
});

/**
 * Client kẹt: TCP socket thô làm handshake WebSocket bằng tay rồi pause, nên không bao giờ đọc dữ liệu
 * và không bao giờ trả lời ping. Dùng socket thô vì thư viện `ws` luôn đọc và trả pong tự động.
 */
async function connectStuck(server: RealtimeServer, room: string): Promise<net.Socket> {
  const target = new URL(server.wsUrl(room));
  const socket = net.connect({ host: target.hostname, port: Number(target.port) });
  rawSockets.push(socket);
  socket.on("error", () => undefined);
  await new Promise<void>((resolve, reject) => {
    socket.once("connect", resolve);
    socket.once("error", reject);
  });
  socket.write(
    `GET ${target.pathname}${target.search} HTTP/1.1\r\n` +
      `Host: ${target.host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n` +
      `Sec-WebSocket-Key: ${randomBytes(16).toString("base64")}\r\nSec-WebSocket-Version: 13\r\n\r\n`,
  );
  await new Promise<void>((resolve, reject) => {
    let head = "";
    const onData = (chunk: Buffer) => {
      head += chunk.toString("latin1");
      if (!head.includes("\r\n\r\n")) return;
      socket.off("data", onData);
      socket.pause();
      if (head.startsWith("HTTP/1.1 101")) resolve();
      else reject(new Error(`handshake bị từ chối: ${head.split("\r\n")[0]}`));
    };
    socket.on("data", onData);
    socket.once("close", () => reject(new Error("socket đóng trước khi handshake xong")));
  });
  return socket;
}
