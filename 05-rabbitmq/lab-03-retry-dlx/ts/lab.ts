import { connect } from "amqplib";
import type {
  Channel,
  ChannelModel,
  ConfirmChannel,
  ConsumeMessage,
  MessagePropertyHeaders,
} from "amqplib";

/** URL của broker. Lab đọc `AMQP_URL`, mặc định là RabbitMQ của `make up`. */
export function amqpUrl(): string {
  return process.env.AMQP_URL ?? "amqp://guest:guest@127.0.0.1:5672";
}

/**
 * Mở một connection tới broker. Listener 'error' rỗng để lỗi connection không làm sập process
 * (EventEmitter ném lỗi nếu 'error' không có listener), lỗi vẫn được báo qua 'close' và Promise của channel.
 */
export async function openConnection(): Promise<ChannelModel> {
  const connection = await connect(amqpUrl());
  connection.on("error", () => undefined);
  return connection;
}

export interface RetryOptions {
  /** Số lần retry tối đa sau lần xử lý đầu tiên. 0 nghĩa là lỗi đầu tiên đi thẳng vào DLQ. */
  maxRetries: number;
  /** Thời gian message nằm chờ trong queue retry trước khi quay lại work queue, tính bằng ms. */
  retryDelayMs: number;
}

/**
 * Tên ba queue của topology, và `maxRetries` để worker biết khi nào bỏ cuộc.
 * Mỗi queue có một direct exchange cùng tên: `<name>.work`, `<name>.retry`, `<name>.dlq`.
 */
export interface RetryQueues {
  work: string;
  retry: string;
  dlq: string;
  maxRetries: number;
}

// Routing key cố định cho từng chặng. Đặt `x-dead-letter-routing-key` tường minh thay vì dùng routing key gốc
// để vòng work -> retry -> work không phụ thuộc vào key mà publisher chọn.
const WORK_KEY = "task";
const RETRY_KEY = "retry";
const DLQ_KEY = "dead";

/**
 * Dựng topology retry bằng DLX và TTL (cả ba queue đều là quorum, khai báo tường minh):
 *
 *   publisher -> exchange work -> queue work --(reject requeue=false: dead-letter)--> exchange retry
 *   exchange retry -> queue retry (không có consumer, `x-message-ttl` = retryDelayMs)
 *   queue retry --(hết TTL: dead-letter)--> exchange work -> queue work (thử lại)
 *   worker đã hết lượt retry -> exchange dlq -> queue dlq (parking lot, cần người xử lý)
 *
 * Queue retry dùng một TTL cố định cho mọi message. Message hết hạn ở đầu queue, nên trộn nhiều TTL khác nhau
 * trong cùng một queue sẽ làm message TTL ngắn bị chặn sau message TTL dài. Muốn nhiều mức delay thì dùng một queue retry cho mỗi mức.
 */
export async function setupRetryTopology(
  channel: Channel,
  name: string,
  opts: RetryOptions,
): Promise<RetryQueues> {
  if (!Number.isInteger(opts.maxRetries) || opts.maxRetries < 0) {
    throw new RangeError(`maxRetries phải là số nguyên không âm, nhận ${opts.maxRetries}`);
  }
  if (!Number.isInteger(opts.retryDelayMs) || opts.retryDelayMs <= 0) {
    throw new RangeError(`retryDelayMs phải là số nguyên dương, nhận ${opts.retryDelayMs}`);
  }
  const queues: RetryQueues = {
    work: `${name}.work`,
    retry: `${name}.retry`,
    dlq: `${name}.dlq`,
    maxRetries: opts.maxRetries,
  };
  const quorum = { "x-queue-type": "quorum" };
  for (const exchange of [queues.work, queues.retry, queues.dlq]) {
    await channel.assertExchange(exchange, "direct", { durable: true });
  }
  await channel.assertQueue(queues.work, {
    durable: true,
    arguments: {
      ...quorum,
      "x-dead-letter-exchange": queues.retry,
      "x-dead-letter-routing-key": RETRY_KEY,
    },
  });
  await channel.assertQueue(queues.retry, {
    durable: true,
    arguments: {
      ...quorum,
      "x-message-ttl": opts.retryDelayMs,
      "x-dead-letter-exchange": queues.work,
      "x-dead-letter-routing-key": WORK_KEY,
    },
  });
  await channel.assertQueue(queues.dlq, { durable: true, arguments: quorum });
  await channel.bindQueue(queues.work, queues.work, WORK_KEY);
  await channel.bindQueue(queues.retry, queues.retry, RETRY_KEY);
  await channel.bindQueue(queues.dlq, queues.dlq, DLQ_KEY);
  return queues;
}

/** Publish một task vào exchange work (persistent) và chờ broker confirm. */
export async function publishWork(
  channel: ConfirmChannel,
  queues: RetryQueues,
  body: string,
): Promise<void> {
  await publishConfirmed(channel, queues.work, WORK_KEY, Buffer.from(body), {});
}

/**
 * Số lần message đã thất bại và bị reject khỏi work queue, đọc từ header `x-death`.
 * `x-death` là array các table, mỗi phần tử gom theo cặp {queue, reason} với `count` là số lần dead-letter.
 * Chỉ tính phần tử của queue work với reason `rejected`, vì phần tử của queue retry (reason `expired`) đếm cùng các lần đó.
 */
export function attemptsOf(headers: MessagePropertyHeaders | undefined, workQueue: string): number {
  let total = 0;
  for (const death of headers?.["x-death"] ?? []) {
    if (death.queue === workQueue && death.reason === "rejected") total += death.count;
  }
  return total;
}

export interface WorkerOptions {
  consumerTag?: string;
}

/**
 * Chạy một worker trên queue work, mỗi lần xử lý một message (prefetch 1).
 *
 * - Handler thành công: ack.
 * - Handler lỗi và `attempts < maxRetries`: `reject(requeue=false)`. Broker dead-letter message sang exchange retry,
 *   message nằm trong queue retry đủ `retryDelayMs` rồi tự quay lại work. Phải dùng reject chứ không dùng nack
 *   khi cần x-death tăng, và từ 4.3 chỉ reject mới tính vào delivery limit.
 * - Handler lỗi và `attempts >= maxRetries`: publish một bản sao sang DLQ, giữ nguyên header (kể cả `x-death`)
 *   cộng `x-failure-reason` (lỗi cuối cùng) và `x-attempts` (tổng số lần đã xử lý), chờ broker confirm rồi mới ack bản gốc.
 *   Nếu publish sang DLQ không được confirm thì `reject(requeue=true)`: message không bao giờ bị mất, tệ nhất là
 *   xuất hiện hai bản ở DLQ nếu worker chết giữa lúc confirm và ack (at-least-once).
 *
 * Tổng số lần handler được gọi cho một message luôn bằng `maxRetries + 1`.
 * `channel` phải là confirm channel vì worker publish sang DLQ trên chính channel này.
 */
export async function startWorker(
  channel: ConfirmChannel,
  queues: RetryQueues,
  handler: (msg: Buffer) => Promise<void>,
  options: WorkerOptions = {},
): Promise<string> {
  channel.on("error", () => undefined);
  await channel.prefetch(1);
  const { consumerTag } = await channel.consume(
    queues.work,
    (msg) => {
      // msg là null khi consumer bị cancel từ phía broker (ví dụ queue bị xóa).
      if (msg === null) return;
      // Ack hoặc reject ném lỗi khi channel đã đóng. Khi đó broker đã tự requeue message nên bỏ qua.
      handle(channel, queues, handler, msg).catch(() => undefined);
    },
    { noAck: false, ...(options.consumerTag ? { consumerTag: options.consumerTag } : {}) },
  );
  return consumerTag;
}

async function handle(
  channel: ConfirmChannel,
  queues: RetryQueues,
  handler: (msg: Buffer) => Promise<void>,
  msg: ConsumeMessage,
): Promise<void> {
  let failure: unknown;
  try {
    await handler(msg.content);
  } catch (error) {
    failure = error;
  }
  if (failure === undefined) {
    channel.ack(msg);
    return;
  }
  const attempts = attemptsOf(msg.properties.headers, queues.work);
  if (attempts < queues.maxRetries) {
    channel.reject(msg, false);
    return;
  }
  try {
    await publishConfirmed(channel, queues.dlq, DLQ_KEY, msg.content, {
      headers: {
        ...msg.properties.headers,
        "x-failure-reason": failure instanceof Error ? failure.message : String(failure),
        "x-attempts": attempts + 1,
      },
    });
  } catch {
    channel.reject(msg, true);
    return;
  }
  channel.ack(msg);
}

function publishConfirmed(
  channel: ConfirmChannel,
  exchange: string,
  routingKey: string,
  content: Buffer,
  options: { headers?: Record<string, unknown> },
): Promise<void> {
  return new Promise((resolve, reject) => {
    channel.publish(exchange, routingKey, content, { persistent: true, ...options }, (err) =>
      err ? reject(err) : resolve(),
    );
  });
}
