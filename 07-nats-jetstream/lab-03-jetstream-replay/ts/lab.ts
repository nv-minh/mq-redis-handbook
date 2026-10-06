import { connect, nanos } from "@nats-io/transport-node";
import type { NatsConnection } from "@nats-io/transport-node";
import {
  AckPolicy,
  DeliverPolicy,
  JetStreamApiCodes,
  JetStreamApiError,
  RetentionPolicy,
  StorageType,
  jetstream,
  jetstreamManager,
} from "@nats-io/jetstream";
import type { Consumer } from "@nats-io/jetstream";

/** URL của server. Lab đọc `NATS_URL`, mặc định là NATS của `make up`. */
export function natsUrl(): string {
  return process.env.NATS_URL ?? "nats://127.0.0.1:4222";
}

/** Subject của stream: tên stream và subject đều xuất phát từ cùng một `name`. */
export function subjectOf(name: string): string {
  return `${name}.events`;
}

// Connection mà lab mở cho từng `name` (mỗi reader giữ một connection), để teardown đóng được chúng.
const connections = new Map<string, NatsConnection[]>();

async function openFor(name: string): Promise<NatsConnection> {
  const nc = await connect({ servers: natsUrl() });
  const list = connections.get(name) ?? [];
  list.push(nc);
  connections.set(name, list);
  return nc;
}

/**
 * Tạo stream memory với retention `limits`: ack và đọc không xóa message, nên lịch sử còn nguyên để replay.
 * Storage, retention và deliver policy cố định từ lúc tạo, không sửa được sau này.
 */
export async function createReplayStream(name: string): Promise<void> {
  const nc = await openFor(name);
  const jsm = await jetstreamManager(nc);
  await jsm.streams.add({
    name,
    subjects: [subjectOf(name)],
    storage: StorageType.Memory,
    retention: RetentionPolicy.Limits,
  });
}

/** Publish các body vào stream, chờ PubAck của từng message và trả về stream sequence do server gán. */
export async function publishBatch(name: string, bodies: string[]): Promise<number[]> {
  const nc = await connect({ servers: natsUrl() });
  try {
    const js = jetstream(nc);
    const seqs: number[] = [];
    for (const body of bodies) seqs.push((await js.publish(subjectOf(name), body)).seq);
    return seqs;
  } finally {
    await nc.close();
  }
}

/**
 * Timestamp server gán cho message đã lưu, dạng chuỗi ISO gốc của server (độ phân giải nanosecond).
 * Không dùng `StoredMsg.time` vì `Date` của JavaScript chỉ có độ phân giải mili giây.
 */
export async function storedMessageTime(name: string, seq: number): Promise<string> {
  const nc = await connect({ servers: natsUrl() });
  try {
    const stored = await (await jetstreamManager(nc)).streams.getMessage(name, { seq });
    if (stored === null) throw new Error(`stream ${name} không có message với sequence ${seq}`);
    return stored.timestamp;
  } finally {
    await nc.close();
  }
}

/** Đổi chuỗi RFC 3339 (phần giây có thể có tới 9 chữ số) thành số nanosecond kể từ epoch. */
export function parseNanos(timestamp: string): bigint {
  const match = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?Z$/.exec(timestamp);
  if (match === null) throw new Error(`timestamp không đúng định dạng RFC 3339 UTC: ${timestamp}`);
  const seconds = BigInt(Date.parse(`${match[1]}Z`) / 1000);
  return seconds * 1_000_000_000n + BigInt((match[2] ?? "").padEnd(9, "0"));
}

function formatNanos(value: bigint): string {
  const seconds = value / 1_000_000_000n;
  const fraction = (value % 1_000_000_000n).toString().padStart(9, "0");
  return `${new Date(Number(seconds) * 1000).toISOString().slice(0, 19)}.${fraction}Z`;
}

/**
 * Mốc thời gian nằm giữa hai timestamp của server: `older < kết quả <= newer`.
 * Cả hai đầu vào đều là timestamp do server gán nên kết quả không phụ thuộc đồng hồ của client.
 */
export function startTimeBetween(older: string, newer: string): string {
  const lo = parseNanos(older);
  const hi = parseNanos(newer);
  if (hi <= lo) throw new Error(`cần older < newer, nhận được ${older} và ${newer}`);
  return formatNanos(lo + (hi - lo + 1n) / 2n);
}

/** Deliver policy của consumer replay: `all` hoặc `by_start_time` kèm mốc RFC 3339. */
export type ReplayPolicy = { kind: "all" } | { kind: "by_start_time"; startTime: string };

export interface ReplayedMessage {
  /** Stream sequence do server gán. */
  seq: number;
  body: string;
}

export interface ReplayReader {
  /** Số message consumer còn phải giao ngay sau khi tạo (`num_pending`): với by_start_time đó là số message có timestamp >= mốc. */
  readonly pendingAtStart: number;
  /** Đọc hết những gì consumer có thể giao lúc này, rồi dừng khi một fetch có hạn trả về rỗng. */
  read(): Promise<ReplayedMessage[]>;
}

const PROBE_EXPIRES_MS = 1_000;

/**
 * Tạo một consumer MỚI (ephemeral pull consumer, ack none) với deliver policy đã chọn.
 *
 * Deliver policy chỉ áp dụng lúc consumer được tạo và không sửa được: muốn đọc lại từ chỗ khác thì tạo consumer mới.
 * Ack none vì replay chỉ đọc: không có pending list, không AckWait, không redelivery.
 * Consumer bị xóa cùng stream khi `teardown(name)`.
 */
export async function openReplay(name: string, policy: ReplayPolicy): Promise<ReplayReader> {
  const nc = await openFor(name);
  const jsm = await jetstreamManager(nc);
  const info = await jsm.consumers.add(name, {
    ack_policy: AckPolicy.None,
    ...(policy.kind === "all"
      ? { deliver_policy: DeliverPolicy.All }
      : { deliver_policy: DeliverPolicy.StartTime, opt_start_time: policy.startTime }),
    // Ephemeral consumer bị server xóa khi không có pull request nào trong khoảng này.
    inactive_threshold: nanos(60_000),
  });
  const consumer: Consumer = await jetstream(nc).consumers.get(name, info.name);
  return {
    pendingAtStart: info.num_pending,
    read: async () => {
      const out: ReplayedMessage[] = [];
      const collect = async (max: number, expires: number) => {
        const batch = await consumer.fetch({ max_messages: max, expires });
        let received = 0;
        for await (const m of batch) {
          out.push({ seq: m.info.streamSequence, body: m.string() });
          received++;
        }
        return received;
      };
      // Số message còn phải giao biết trước từ server, nên lần fetch đầu trả về ngay khi đủ, không chờ hết hạn.
      const { num_pending: pending } = await consumer.info();
      if (pending > 0) await collect(pending, 5_000);
      // Fetch dò: server không còn gì thì trả về rỗng sau PROBE_EXPIRES_MS. Nếu còn thừa thì message thừa lộ ra ở kết quả.
      while ((await collect(100, PROBE_EXPIRES_MS)) > 0);
      return out;
    },
  };
}

/** Xóa stream (kéo theo xóa mọi consumer) bằng một connection mới rồi đóng mọi connection của `name`. Chịu được stream chưa tạo. */
export async function teardown(name: string): Promise<void> {
  const open = connections.get(name) ?? [];
  connections.delete(name);
  let failure: unknown;
  try {
    const nc = await connect({ servers: natsUrl() });
    try {
      await (await jetstreamManager(nc)).streams.delete(name);
    } catch (error) {
      const notFound =
        error instanceof JetStreamApiError &&
        error.apiError().err_code === JetStreamApiCodes.StreamNotFound;
      if (!notFound) throw error;
    } finally {
      await nc.close();
    }
  } catch (error) {
    failure = error;
  }
  const closing = await Promise.allSettled(open.map((nc) => nc.close()));
  failure ??= closing.find((r) => r.status === "rejected")?.reason;
  if (failure !== undefined) throw failure;
}
