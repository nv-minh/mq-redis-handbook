import { connect } from "@nats-io/transport-node";
import type { NatsConnection, Subscription } from "@nats-io/transport-node";

/** URL của server. Lab đọc `NATS_URL`, mặc định là NATS của `make up`. */
export function natsUrl(): string {
  return process.env.NATS_URL ?? "nats://127.0.0.1:4222";
}

/** Mở một connection core NATS. Package `@nats-io/transport-node` 3.4.0 thay cho package `nats` cũ đã bị deprecate. */
export async function openConnection(): Promise<NatsConnection> {
  return connect({ servers: natsUrl() });
}

/** Một subscriber đang gom message vào `received` theo đúng thứ tự nhận. */
export interface Receiver {
  readonly subscription: Subscription;
  readonly received: string[];
  /** Resolve khi vòng đọc kết thúc (subscription bị drain, unsubscribe hoặc connection đóng). */
  readonly done: Promise<void>;
}

/**
 * Subscribe `subject` (kèm tên queue group nếu có) và gom body của mọi message nhận được.
 *
 * Hàm flush trước khi return: PONG của server chứng tỏ server đã xử lý xong lệnh SUB,
 * nên publish ngay sau đó chắc chắn thấy subscriber này. Đây là cách làm "subscriber đã sẵn sàng"
 * mà không cần sleep.
 */
export async function subscribeCollect(
  nc: NatsConnection,
  subject: string,
  queue?: string,
): Promise<Receiver> {
  const subscription = nc.subscribe(subject, queue ? { queue } : {});
  const received: string[] = [];
  // Vòng đọc kết thúc cùng subscription. Lỗi không được thành unhandled rejection khi test đã teardown.
  const done = (async () => {
    for await (const msg of subscription) received.push(msg.string());
  })().catch(() => undefined);
  await nc.flush();
  return { subscription, received, done };
}

/**
 * Publish từng body lên `subject` rồi flush.
 * Publish của core NATS chỉ ghi vào buffer của client và không có ack; flush chờ server xác nhận
 * đã đọc hết, nhưng vẫn không cho biết có ai nhận message hay không.
 */
export async function publishAll(
  nc: NatsConnection,
  subject: string,
  bodies: string[],
): Promise<void> {
  for (const body of bodies) nc.publish(subject, body);
  await nc.flush();
}
