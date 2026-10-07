import { randomBytes } from "node:crypto";
import http from "node:http";
import type { AddressInfo } from "node:net";
import type { Duplex } from "node:stream";
import { Redis } from "ioredis";
import WebSocket, { WebSocketServer } from "ws";

/** Message mà client nhận được. Server điền `from` và `ts`, client chỉ gửi `{ "text": "..." }`. */
export interface ChatMessage {
  room: string;
  from: string;
  text: string;
  ts: number;
}

export interface RealtimeOptions {
  redisUrl: string;
  /** Prefix của mọi channel và key (tách theo môi trường, và để test dùng `uniqueName`). */
  prefix: string;
  /**
   * Giới hạn kích thước MỘT message client gửi lên, tính bằng byte của frame. Mặc định 4096.
   * Thư viện `ws` mặc định tới 100 MiB (maxPayload), nên phải đặt tường minh.
   */
  maxMessageBytes?: number;
  /**
   * Chính sách client chậm: nếu dữ liệu đang chờ gửi cho một client vượt ngưỡng này (byte) thì
   * server hủy connection của nó thay vì để bộ nhớ tăng mãi. Mặc định 262144.
   */
  maxBufferedBytes?: number;
  /** Chu kỳ ping heartbeat. Client không trả pong trong một chu kỳ thì bị hủy. Mặc định 30000. */
  pingIntervalMs?: number;
  /** Thời gian chờ close handshake khi đóng một connection trước khi hủy hẳn. Mặc định 2000. */
  closeGraceMs?: number;
  /** Nhận các lỗi không có chỗ nào khác để báo (Redis, socket). Mặc định bỏ qua. */
  onError?: (error: Error) => void;
}

export interface RealtimeServer {
  /** URL HTTP gốc, ví dụ http://127.0.0.1:41234. */
  url: string;
  /** URL WebSocket của một phòng, kèm tên hiển thị tùy chọn. */
  wsUrl(room: string, name?: string): string;
  /** Số client cục bộ (trên instance này) đang ở trong phòng. */
  clientCount(room: string): number;
  /** Số phòng mà instance này đang subscribe (phòng có ít nhất một client cục bộ). */
  roomCount(): number;
  /** Trạng thái hai connection Redis của instance (ioredis status), để test kiểm tra đã đóng. */
  redisStatus(): { command: string; subscriber: string };
  /** Đóng mọi thứ. Idempotent: gọi nhiều lần (kể cả đồng thời) trả về cùng một kết quả. */
  shutdown(): Promise<void>;
}

/** Tên room, tên hiển thị và tên bảng xếp hạng: 1 đến 64 ký tự an toàn để ghép vào tên channel và key. */
const NAME_PATTERN = /^[A-Za-z0-9._-]{1,64}$/;
const NAME_RULE = "gồm 1 đến 64 ký tự trong [A-Za-z0-9._-]";
const MAX_MEMBER_LENGTH = 128;
const MAX_TOP = 1000;
const DEFAULT_TOP = 10;
const MAX_BODY_BYTES = 1024;

/** Channel Pub/Sub của một phòng. Prefix lấy từ options nên mỗi môi trường và mỗi test có channel riêng. */
export function roomChannel(prefix: string, room: string): string {
  return `${prefix}:room:${room}`;
}

/** Key sorted set của một bảng xếp hạng. */
export function leaderboardKey(prefix: string, name: string): string {
  return `${prefix}:lb:${name}`;
}

interface Room {
  name: string;
  channel: string;
  clients: Set<Client>;
  /** Client trong phòng cộng các lần upgrade đang chờ subscribe xong. Về 0 thì unsubscribe. */
  refs: number;
  /** Resolve khi Redis xác nhận SUBSCRIBE: từ đó mọi PUBLISH tới channel đều tới instance này. */
  ready: Promise<void>;
}

interface Client {
  ws: WebSocket;
  name: string;
  room: Room;
  /** false từ lúc gửi ping tới lúc nhận pong. */
  alive: boolean;
  heartbeat: NodeJS.Timeout;
  forceClose?: NodeJS.Timeout;
  closed: Promise<void>;
}

class HttpError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

/**
 * Một instance server realtime: HTTP + WebSocket trên cổng 0 (hệ điều hành chọn cổng), một connection Redis
 * cho lệnh thường và MỘT connection dành riêng cho subscribe. Chạy nhiều instance trong cùng process để mô
 * phỏng nhiều máy dùng chung một Redis.
 */
export async function createRealtimeServer(options: RealtimeOptions): Promise<RealtimeServer> {
  const {
    redisUrl,
    prefix,
    maxMessageBytes = 4096,
    maxBufferedBytes = 262_144,
    pingIntervalMs = 30_000,
    closeGraceMs = 2000,
    onError = () => undefined,
  } = options;
  const channelPrefix = `${prefix}:room:`;

  const commands = new Redis(redisUrl);
  // Subscriber dành riêng: một connection đã subscribe không dùng chung với lệnh thường.
  const subscriber = new Redis(redisUrl);
  for (const connection of [commands, subscriber]) {
    connection.on("error", (error: Error) => onError(error));
  }
  try {
    await commands.ping();
  } catch (error) {
    commands.disconnect();
    subscriber.disconnect();
    throw error;
  }

  const rooms = new Map<string, Room>();
  const allClients = new Set<Client>();
  let closing = false;

  // ---- Phòng và vòng đời subscribe ----

  /**
   * Giữ chỗ trong phòng: subscribe channel khi là lần đầu (SUBSCRIBE được gửi NGAY, đồng bộ với thay đổi
   * trạng thái, nên thứ tự SUBSCRIBE/UNSUBSCRIBE trên connection luôn đúng thứ tự các sự kiện).
   */
  function acquireRoom(name: string): Room {
    let room = rooms.get(name);
    if (!room) {
      const channel = roomChannel(prefix, name);
      const ready = subscriber.subscribe(channel).then(() => undefined);
      // Lỗi subscribe được báo cho client đang chờ ready; gắn catch để không thành unhandled rejection.
      ready.catch(() => undefined);
      room = { name, channel, clients: new Set(), refs: 0, ready };
      rooms.set(name, room);
    }
    room.refs++;
    return room;
  }

  function releaseRoom(room: Room): void {
    room.refs--;
    if (room.refs > 0 || rooms.get(room.name) !== room) return;
    rooms.delete(room.name);
    if (subscriber.status === "ready") {
      subscriber.unsubscribe(room.channel).catch((error: Error) => onError(error));
    }
  }

  // ---- Fan-out: mọi message tới client đều đi qua Redis, kể cả với client cùng instance với người gửi ----

  subscriber.on("message", (channel: string, payload: string) => {
    if (!channel.startsWith(channelPrefix)) return;
    const room = rooms.get(channel.slice(channelPrefix.length));
    if (!room) return;
    for (const client of room.clients) deliver(client, payload);
  });

  function deliver(client: Client, payload: string): void {
    const { ws } = client;
    // Không bao giờ gửi vào socket đã đóng.
    if (ws.readyState !== WebSocket.OPEN) return;
    // Chính sách client chậm: hàng đợi gửi vượt ngưỡng thì hủy client này. Các client khác không bị ảnh hưởng
    // vì ws.send không chặn. Không gửi close frame vì nó cũng nằm sau hàng đợi và client sẽ không đọc tới.
    if (ws.bufferedAmount > maxBufferedBytes) {
      ws.terminate();
      return;
    }
    ws.send(payload, (error) => {
      if (error) ws.terminate();
    });
  }

  // ---- WebSocket ----

  const wss = new WebSocketServer({
    noServer: true,
    maxPayload: maxMessageBytes,
    // permessage-deflate tốn CPU và bộ nhớ cho mỗi connection, và message chat nhỏ không cần nó.
    perMessageDeflate: false,
  });

  function join(room: Room, ws: WebSocket, name: string): void {
    let markClosed!: () => void;
    const closed = new Promise<void>((resolve) => (markClosed = resolve));
    const client: Client = {
      ws,
      name,
      room,
      alive: true,
      closed,
      heartbeat: setInterval(() => {
        if (!client.alive) {
          // Không trả pong từ chu kỳ trước: coi là đã chết (rút dây mạng, NAT timeout).
          ws.terminate();
          return;
        }
        client.alive = false;
        try {
          ws.ping();
        } catch (error) {
          onError(error as Error);
        }
      }, pingIntervalMs),
    };
    room.clients.add(client);
    allClients.add(client);
    // Socket `ws` ném 'error' không có listener sẽ làm sập cả process, nên luôn gắn listener.
    ws.on("error", (error) => onError(error));
    ws.on("pong", () => (client.alive = true));
    ws.on("message", (data, isBinary) => onMessage(client, data, isBinary));
    ws.on("close", () => {
      clearInterval(client.heartbeat);
      clearTimeout(client.forceClose);
      if (room.clients.delete(client)) {
        allClients.delete(client);
        releaseRoom(room);
      }
      markClosed();
    });
  }

  /** Đóng bằng close frame có mã, và hủy hẳn nếu client không trả lời handshake trong closeGraceMs. */
  function closeWith(client: Client, code: number, reason: string): void {
    const { ws } = client;
    if (ws.readyState !== WebSocket.OPEN) return;
    ws.close(code, reason);
    client.forceClose = setTimeout(() => ws.terminate(), closeGraceMs);
  }

  function onMessage(client: Client, data: WebSocket.RawData, isBinary: boolean): void {
    if (client.ws.readyState !== WebSocket.OPEN) return;
    const text = isBinary ? undefined : parseText(data);
    if (text === undefined) {
      // 1003 (Unsupported Data): server nhận kiểu dữ liệu nó không chấp nhận. Chỉ connection này bị đóng.
      closeWith(client, 1003, 'chỉ nhận text JSON dạng {"text": "..."} không rỗng');
      return;
    }
    const message: ChatMessage = {
      room: client.room.name,
      from: client.name,
      text,
      ts: Date.now(),
    };
    commands.publish(client.room.channel, JSON.stringify(message)).catch((error: Error) => {
      onError(error);
      // 1011 (Internal Error): không publish được thì message sẽ mất, nên báo cho client biết.
      closeWith(client, 1011, "không gửi được message, Redis lỗi");
    });
  }

  // ---- HTTP: WebSocket upgrade và API bảng xếp hạng ----

  const httpServer = http.createServer((request, response) => {
    handleHttp(request, response).catch((error: unknown) => {
      if (response.headersSent) return void response.end();
      if (error instanceof HttpError) {
        if (error.status === 413) response.setHeader("Connection", "close");
        sendJson(response, error.status, { error: error.message });
        return;
      }
      onError(error as Error);
      sendJson(response, 500, { error: "lỗi nội bộ, xem log của server" });
    });
  });

  httpServer.on("upgrade", (request, socket, head) => {
    // Lỗi của socket trước khi upgrade xong (client bỏ đi giữa chừng) không được làm sập process.
    socket.on("error", () => socket.destroy());
    handleUpgrade(request, socket, head).catch((error: unknown) => {
      onError(error as Error);
      socket.destroy();
    });
  });

  async function handleUpgrade(
    request: http.IncomingMessage,
    socket: Duplex,
    head: Buffer,
  ): Promise<void> {
    const target = new URL(request.url ?? "/", "http://localhost");
    if (target.pathname !== "/ws") return rejectUpgrade(socket, 404, "không có endpoint này");
    // Validate TRƯỚC khi upgrade: room sai là HTTP 400 rõ ràng, không phải một WebSocket mở rồi đóng.
    const roomName = target.searchParams.get("room") ?? "";
    if (!NAME_PATTERN.test(roomName)) {
      return rejectUpgrade(socket, 400, `room phải ${NAME_RULE}`);
    }
    const name = target.searchParams.get("name") ?? `anon-${randomBytes(3).toString("hex")}`;
    if (!NAME_PATTERN.test(name)) return rejectUpgrade(socket, 400, `name phải ${NAME_RULE}`);
    // Origin: client không phải trình duyệt thường không gửi Origin nên được nhận. Trình duyệt luôn gửi,
    // và lab chỉ nhận origin cùng host. Production phải liệt kê origin được phép và xác thực người dùng.
    if (!originAllowed(request)) return rejectUpgrade(socket, 403, "origin không được phép");
    if (closing) return rejectUpgrade(socket, 503, "server đang tắt");

    const room = acquireRoom(roomName);
    try {
      // Chỉ nhận kết nối sau khi instance đã subscribe xong: client open nghĩa là mọi message về sau tới được.
      await room.ready;
    } catch (error) {
      releaseRoom(room);
      onError(error as Error);
      return rejectUpgrade(socket, 503, "không subscribe được channel của phòng");
    }
    if (closing || socket.destroyed) {
      releaseRoom(room);
      if (!socket.destroyed) rejectUpgrade(socket, 503, "server đang tắt");
      return;
    }
    wss.handleUpgrade(request, socket, head, (ws) => join(room, ws, name));
  }

  async function handleHttp(
    request: http.IncomingMessage,
    response: http.ServerResponse,
  ): Promise<void> {
    const path = new URL(request.url ?? "/", "http://localhost");
    const route = /^\/leaderboard\/([^/]+)(\/top)?$/.exec(path.pathname);
    if (!route) {
      if (path.pathname === "/ws") throw new HttpError(426, "endpoint này cần WebSocket upgrade");
      throw new HttpError(404, "không có đường dẫn này");
    }
    let name: string;
    try {
      name = decodeURIComponent(route[1]!);
    } catch {
      throw new HttpError(400, "tên bảng xếp hạng không hợp lệ");
    }
    if (!NAME_PATTERN.test(name)) throw new HttpError(400, `name phải ${NAME_RULE}`);
    const key = leaderboardKey(prefix, name);

    if (route[2] === undefined) {
      if (request.method !== "POST") {
        response.setHeader("Allow", "POST");
        throw new HttpError(405, "chỉ hỗ trợ POST");
      }
      const { member, score } = parseScore(await readBody(request));
      // ZADD đặt score (set), không cộng dồn: POST lại cùng member thì thay score cũ.
      await commands.zadd(key, score, member);
      sendJson(response, 200, { name, member, score });
      return;
    }
    if (request.method !== "GET") {
      response.setHeader("Allow", "GET");
      throw new HttpError(405, "chỉ hỗ trợ GET");
    }
    const n = parseTopN(path.searchParams.get("n"));
    // ZRANGE ... REV thay cho ZREVRANGE (đã deprecated). Score bằng nhau thì member theo thứ tự từ điển GIẢM dần.
    const reply = (await commands.call("ZRANGE", key, 0, n - 1, "REV", "WITHSCORES")) as unknown[];
    sendJson(response, 200, { name, entries: toEntries(reply) });
  }

  function originAllowed(request: http.IncomingMessage): boolean {
    const origin = request.headers.origin;
    if (origin === undefined) return true;
    try {
      return new URL(origin).host.toLowerCase() === (request.headers.host ?? "").toLowerCase();
    } catch {
      return false;
    }
  }

  await new Promise<void>((resolve, reject) => {
    httpServer.once("error", reject);
    httpServer.listen(0, "127.0.0.1", resolve);
  }).catch((error: unknown) => {
    commands.disconnect();
    subscriber.disconnect();
    throw error;
  });
  const port = (httpServer.address() as AddressInfo).port;
  const url = `http://127.0.0.1:${port}`;

  // ---- Shutdown ----

  let shutdownPromise: Promise<void> | undefined;

  async function doShutdown(): Promise<void> {
    closing = true;
    // Mỗi pha chạy dù pha trước lỗi; lỗi được gom lại và ném ở cuối.
    const errors: unknown[] = [];
    const step = async (fn: () => Promise<unknown>): Promise<void> => {
      try {
        await fn();
      } catch (error) {
        errors.push(error);
      }
    };
    // Pha 1: ngừng nhận kết nối mới (listener đóng ngay, callback chờ nốt các connection đang mở).
    const httpClosed = new Promise<void>((resolve, reject) => {
      httpServer.close((error) => (error ? reject(error) : resolve()));
    });
    // Pha 2: đóng mọi WebSocket bằng 1001 (Going Away), chờ close handshake, rồi hủy những socket còn lại.
    // Mỗi client rời phòng qua đường 'close' bình thường nên phòng cuối cùng sẽ UNSUBSCRIBE.
    await step(async () => {
      const open = [...allClients];
      for (const client of open) closeWith(client, 1001, "server đang tắt");
      let timer: NodeJS.Timeout | undefined;
      const grace = new Promise<void>((resolve) => (timer = setTimeout(resolve, closeGraceMs)));
      await Promise.race([Promise.all(open.map((client) => client.closed)), grace]);
      clearTimeout(timer);
      for (const client of open) client.ws.terminate();
      await Promise.all(open.map((client) => client.closed));
    });
    // Pha 3: đóng hai connection Redis. QUIT đi sau các UNSUBSCRIBE nên chúng được gửi đi trước.
    await step(() => closeRedis(subscriber));
    await step(() => closeRedis(commands));
    // Pha 4: đóng nốt các connection HTTP còn lại (keep-alive) và chờ server dừng hẳn.
    await step(() => {
      httpServer.closeAllConnections();
      return httpClosed;
    });
    wss.close();
    if (errors.length === 1) throw errors[0];
    if (errors.length > 1) throw new AggregateError(errors, "shutdown gặp nhiều lỗi");
  }

  return {
    url,
    wsUrl: (room, name) =>
      `ws://127.0.0.1:${port}/ws?room=${encodeURIComponent(room)}` +
      (name === undefined ? "" : `&name=${encodeURIComponent(name)}`),
    clientCount: (room) => rooms.get(room)?.clients.size ?? 0,
    roomCount: () => rooms.size,
    redisStatus: () => ({ command: commands.status, subscriber: subscriber.status }),
    shutdown: () => (shutdownPromise ??= doShutdown()),
  };
}

/** QUIT rồi chờ socket đóng hẳn (status "end"): reply OK của QUIT về trước khi socket đóng. */
async function closeRedis(connection: Redis): Promise<void> {
  const ended = new Promise<void>((resolve) => {
    if (connection.status === "end") resolve();
    else connection.once("end", resolve);
  });
  try {
    await connection.quit();
    await ended;
  } catch (error) {
    connection.disconnect();
    throw error;
  }
}

function rejectUpgrade(socket: Duplex, status: number, message: string): void {
  const body = `${message}\n`;
  socket.end(
    `HTTP/1.1 ${status} ${http.STATUS_CODES[status] ?? ""}\r\n` +
      "Connection: close\r\nContent-Type: text/plain; charset=utf-8\r\n" +
      `Content-Length: ${Buffer.byteLength(body)}\r\n\r\n${body}`,
  );
}

function sendJson(response: http.ServerResponse, status: number, body: unknown): void {
  response.writeHead(status, { "Content-Type": "application/json" }).end(JSON.stringify(body));
}

/** Lấy `text` từ frame client gửi, hoặc undefined nếu không phải JSON object có `text` là chuỗi không rỗng. */
function parseText(data: WebSocket.RawData): string | undefined {
  const raw = Array.isArray(data) ? Buffer.concat(data) : Buffer.from(data as Buffer);
  try {
    const parsed: unknown = JSON.parse(raw.toString("utf8"));
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return undefined;
    const text = (parsed as { text?: unknown }).text;
    return typeof text === "string" && text.length > 0 ? text : undefined;
  } catch {
    return undefined;
  }
}

async function readBody(request: http.IncomingMessage): Promise<string> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of request) {
    size += (chunk as Buffer).length;
    if (size > MAX_BODY_BYTES) throw new HttpError(413, `body vượt quá ${MAX_BODY_BYTES} byte`);
    chunks.push(chunk as Buffer);
  }
  return Buffer.concat(chunks).toString("utf8");
}

function parseScore(raw: string): { member: string; score: number } {
  let body: unknown;
  try {
    body = JSON.parse(raw);
  } catch {
    throw new HttpError(400, "body không phải JSON hợp lệ");
  }
  const { member, score } = (typeof body === "object" && body !== null ? body : {}) as {
    member?: unknown;
    score?: unknown;
  };
  if (typeof member !== "string" || member.length < 1 || member.length > MAX_MEMBER_LENGTH) {
    throw new HttpError(400, `member phải là chuỗi dài 1 đến ${MAX_MEMBER_LENGTH} ký tự`);
  }
  if (typeof score !== "number" || !Number.isFinite(score)) {
    throw new HttpError(400, "score phải là một số hữu hạn");
  }
  return { member, score };
}

function parseTopN(raw: string | null): number {
  if (raw === null) return DEFAULT_TOP;
  const n = /^[0-9]+$/.test(raw) ? Number(raw) : NaN;
  if (!Number.isSafeInteger(n) || n < 1 || n > MAX_TOP) {
    throw new HttpError(400, `n phải là số nguyên từ 1 đến ${MAX_TOP}`);
  }
  return n;
}

/**
 * Chuẩn hóa reply của ZRANGE ... WITHSCORES. Đã đo với ioredis 6.0.0 (RESP3) trên Redis 8.10.2:
 * xem README. Nhận cả dạng phẳng [member, score, ...] lẫn dạng lồng [[member, score], ...].
 */
function toEntries(reply: unknown[]): { member: string; score: number }[] {
  const entries: { member: string; score: number }[] = [];
  if (reply.length > 0 && Array.isArray(reply[0])) {
    for (const pair of reply as [string, number | string][]) {
      entries.push({ member: pair[0], score: Number(pair[1]) });
    }
    return entries;
  }
  for (let i = 0; i < reply.length; i += 2) {
    entries.push({ member: String(reply[i]), score: Number(reply[i + 1]) });
  }
  return entries;
}
