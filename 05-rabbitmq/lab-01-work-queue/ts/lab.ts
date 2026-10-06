import { connect } from "amqplib";
import type { ChannelModel, Channel, ConfirmChannel, ConsumeMessage } from "amqplib";

/** URL của broker. Lab đọc `AMQP_URL`, mặc định là RabbitMQ của `make up`. */
export function amqpUrl(): string {
  return process.env.AMQP_URL ?? "amqp://guest:guest@127.0.0.1:5672";
}

/**
 * Mở một connection tới broker. API của amqplib 2.2.0 dựa trên Promise và package là CommonJS có sẵn file kiểu.
 *
 * EventEmitter ném lỗi nếu 'error' không có listener, và lỗi đó sẽ làm sập cả process.
 * Lỗi của connection vẫn được báo qua sự kiện 'close' và qua các Promise của channel,
 * nên listener rỗng ở đây chỉ để process không bị sập.
 */
export async function openConnection(): Promise<ChannelModel> {
  const connection = await connect(amqpUrl());
  connection.on("error", () => undefined);
  return connection;
}

/**
 * Khai báo work queue kiểu quorum, durable, không exclusive, không auto-delete.
 *
 * `x-queue-type` được truyền tường minh: compose của handbook đặt `default_queue_type = quorum`,
 * nhưng broker mặc định (stock) là classic, và khai báo lại một queue với type khác sẽ lỗi 406.
 * Quorum queue không thể exclusive, auto-delete hay transient.
 */
export async function declareWorkQueue(channel: Channel, queue: string): Promise<void> {
  await channel.assertQueue(queue, { durable: true, arguments: { "x-queue-type": "quorum" } });
}

/**
 * Publish các task vào queue (qua default exchange, routing key là tên queue) rồi chờ broker confirm.
 * Message là persistent. Promise chỉ resolve khi broker đã nhận hết, nên người gọi không cần sleep.
 */
export async function publishTasks(
  channel: ConfirmChannel,
  queue: string,
  bodies: string[],
): Promise<void> {
  for (const body of bodies) channel.sendToQueue(queue, Buffer.from(body), { persistent: true });
  await channel.waitForConfirms();
}

export interface WorkerOptions {
  /** Số message chưa ack tối đa broker giao cho worker. Bỏ trống hoặc 0 là không giới hạn. */
  prefetch?: number;
  /** Consumer tag. Bỏ trống thì broker tự sinh. */
  consumerTag?: string;
}

/**
 * Chạy một worker với manual ack: handler xong thì ack, handler ném lỗi thì reject(requeue=true).
 * Resolve với consumer tag. `prefetch` quyết định chuyện fair dispatch: với prefetch 1 broker chỉ giao
 * message mới khi message trước đã được ack, nên worker chậm không bị dồn việc.
 *
 * Worker không tự ack khi channel đóng: broker requeue mọi message chưa ack với cờ redelivered.
 */
export async function startWorker(
  channel: Channel,
  queue: string,
  handler: (msg: ConsumeMessage) => Promise<void>,
  options: WorkerOptions = {},
): Promise<string> {
  // Channel lỗi (ví dụ unknown delivery tag) báo qua 'error', listener rỗng để process không bị sập.
  channel.on("error", () => undefined);
  if (options.prefetch !== undefined && options.prefetch > 0)
    await channel.prefetch(options.prefetch);
  const { consumerTag } = await channel.consume(
    queue,
    (msg) => {
      // msg là null khi consumer bị cancel từ phía broker (ví dụ queue bị xóa).
      if (msg === null) return;
      handler(msg).then(
        () => settle(() => channel.ack(msg)),
        () => settle(() => channel.reject(msg, true)),
      );
    },
    { noAck: false, ...(options.consumerTag ? { consumerTag: options.consumerTag } : {}) },
  );
  return consumerTag;
}

/** Ack hoặc reject có thể ném lỗi nếu channel đã đóng. Khi đó broker đã tự requeue message nên bỏ qua. */
function settle(action: () => void): void {
  try {
    action();
  } catch {
    // channel đã đóng, message đã được requeue bởi broker
  }
}
