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
import type { Consumer, ConsumerInfo, JsMsg } from "@nats-io/jetstream";

/** URL của server. Lab đọc `NATS_URL`, mặc định là NATS của `make up`. */
export function natsUrl(): string {
  return process.env.NATS_URL ?? "nats://127.0.0.1:4222";
}

export interface ConsumerOptions {
  /** AckWait: sau chừng này mili giây không ack thì server giao lại message. Phải là số nguyên dương. */
  ackWaitMs: number;
  /** MaxDeliver: số lần giao tối đa. Số nguyên từ 1 trở lên, hoặc -1 là không giới hạn. */
  maxDeliver: number;
}

/** Subject của stream: lab đặt tên stream, subject và durable consumer đều xuất phát từ cùng một `name`. */
export function subjectOf(name: string): string {
  return `${name}.events`;
}

/** Tên durable consumer. Tên consumer chỉ cần duy nhất trong một stream nên dùng luôn `name`. */
export function durableOf(name: string): string {
  return name;
}

// Connection mà lab mở cho từng `name`, để disconnect và teardown đóng được chúng.
// Chữ ký createStreamAndConsumer chỉ trả về Consumer, nên connection của Consumer đó nằm ở đây.
const connections = new Map<string, NatsConnection[]>();

async function openFor(name: string): Promise<NatsConnection> {
  const nc = await connect({ servers: natsUrl() });
  const list = connections.get(name) ?? [];
  list.push(nc);
  connections.set(name, list);
  return nc;
}

/**
 * Server nhận `ack_wait = 0` và im lặng đổi thành 30 s, `max_deliver = 0` hoặc `-2` thành -1 (đã đo trên 2.15.0).
 * Từ chối ở client để giá trị sai không biến thành một giá trị mặc định khác hẳn ý định của người gọi.
 */
function validate(opts: ConsumerOptions): void {
  if (!Number.isInteger(opts.ackWaitMs) || opts.ackWaitMs < 1) {
    throw new Error(
      `ackWaitMs phải là số nguyên dương (mili giây), nhận được ${opts.ackWaitMs}. Server sẽ im lặng đổi 0 thành 30000.`,
    );
  }
  if (!Number.isInteger(opts.maxDeliver) || (opts.maxDeliver < 1 && opts.maxDeliver !== -1)) {
    throw new Error(
      `maxDeliver phải là số nguyên từ 1 trở lên hoặc -1 (không giới hạn), nhận được ${opts.maxDeliver}.`,
    );
  }
}

/**
 * Tạo stream memory (subject duy nhất) và một durable pull consumer với ack explicit, rồi trả về consumer đó.
 *
 * Consumer là object phía server, nên vị trí đọc sống sót khi connection đóng: dùng `bindConsumer` để nối lại.
 * Stream dùng retention `limits` nên ack không xóa message khỏi stream, chỉ đẩy vị trí của consumer.
 * Gọi `teardown(name)` để xóa stream và đóng connection, kể cả khi test fail.
 */
export async function createStreamAndConsumer(
  name: string,
  opts: ConsumerOptions,
): Promise<Consumer> {
  validate(opts);
  const nc = await openFor(name);
  try {
    const jsm = await jetstreamManager(nc);
    await jsm.streams.add({
      name,
      subjects: [subjectOf(name)],
      storage: StorageType.Memory,
      retention: RetentionPolicy.Limits,
    });
    await jsm.consumers.add(name, {
      durable_name: durableOf(name),
      ack_policy: AckPolicy.Explicit,
      deliver_policy: DeliverPolicy.All,
      ack_wait: nanos(opts.ackWaitMs),
      max_deliver: opts.maxDeliver,
    });
    return await jetstream(nc).consumers.get(name, durableOf(name));
  } catch (error) {
    await nc.close().catch(() => undefined);
    throw error;
  }
}

/** Mở một connection mới và bind vào durable consumer đã có: mô phỏng worker khởi động lại sau khi crash. */
export async function bindConsumer(name: string): Promise<Consumer> {
  const nc = await openFor(name);
  try {
    return await jetstream(nc).consumers.get(name, durableOf(name));
  } catch (error) {
    await nc.close().catch(() => undefined);
    throw error;
  }
}

/** Đóng đột ngột mọi connection lab đã mở cho `name` (không drain): mô phỏng worker mất kết nối. */
export async function disconnect(name: string): Promise<void> {
  const open = connections.get(name) ?? [];
  connections.delete(name);
  await Promise.all(open.map((nc) => nc.close()));
}

/** Publish các body vào stream và chờ PubAck của từng message (bằng chứng message đã được lưu). */
export async function publishMessages(name: string, bodies: string[]): Promise<void> {
  const nc = await connect({ servers: natsUrl() });
  try {
    const js = jetstream(nc);
    for (const body of bodies) await js.publish(subjectOf(name), body);
  } finally {
    await nc.close();
  }
}

/**
 * Pull tối đa `max` message, chờ tối đa `expiresMs` mili giây (server giữ pull request nhiều nhất chừng đó).
 * Trả về ngay khi đủ `max` message. Stream rỗng trả về mảng rỗng khi hết hạn, không ném lỗi.
 * `expires` tối thiểu là 1000 ms theo thư viện.
 */
export async function fetchMessages(
  consumer: Consumer,
  max: number,
  expiresMs: number,
): Promise<JsMsg[]> {
  const batch = await consumer.fetch({ max_messages: max, expires: expiresMs });
  const messages: JsMsg[] = [];
  for await (const m of batch) messages.push(m);
  return messages;
}

/** Đọc thông tin consumer từ server qua một connection riêng, nên dùng được cả khi connection của test đã đóng. */
export async function consumerInfo(name: string): Promise<ConsumerInfo> {
  const nc = await connect({ servers: natsUrl() });
  try {
    return await (await jetstreamManager(nc)).consumers.info(name, durableOf(name));
  } finally {
    await nc.close();
  }
}

/** Nội dung advisory `MAX_DELIVERIES` (type `io.nats.jetstream.advisory.v1.max_deliver`). */
export interface MaxDeliveriesAdvisory {
  stream: string;
  consumer: string;
  stream_seq: number;
  deliveries: number;
}

export interface MaxDeliveriesWatch {
  /** Advisory đã nhận, theo thứ tự tới. */
  readonly advisories: MaxDeliveriesAdvisory[];
  /** Round trip tới server: advisory nào server đã gửi trước đó chắc chắn đã nằm trong `advisories`. */
  flush(): Promise<void>;
}

/**
 * Lắng nghe advisory `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.<stream>.<consumer>`.
 *
 * Khi message vượt MaxDeliver, server không giao lại và không có dead-letter queue: advisory này là dấu vết duy nhất.
 * Hàm flush trước khi return, nên publish ngay sau đó chắc chắn được theo dõi.
 */
export async function watchMaxDeliveries(name: string): Promise<MaxDeliveriesWatch> {
  const nc = await openFor(name);
  const subject = `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.${name}.${durableOf(name)}`;
  const subscription = nc.subscribe(subject);
  const advisories: MaxDeliveriesAdvisory[] = [];
  // Vòng đọc kết thúc khi connection đóng. Lỗi không được thành unhandled rejection khi test đã teardown.
  void (async () => {
    for await (const msg of subscription) advisories.push(msg.json<MaxDeliveriesAdvisory>());
  })().catch(() => undefined);
  await nc.flush();
  return { advisories, flush: () => nc.flush() };
}

/**
 * Đóng mọi connection của `name` rồi xóa stream bằng một connection mới (xóa stream kéo theo xóa consumer).
 * Chịu được stream chưa bao giờ được tạo. Lỗi ở một bước không làm bỏ sót các bước sau: lỗi đầu tiên được ném ra ở cuối.
 */
export async function teardown(name: string): Promise<void> {
  const open = connections.get(name) ?? [];
  connections.delete(name);
  const closing = await Promise.allSettled(open.map((nc) => nc.close()));
  let failure: unknown = closing.find((r) => r.status === "rejected")?.reason;
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
    failure ??= error;
  }
  if (failure !== undefined) throw failure;
}
