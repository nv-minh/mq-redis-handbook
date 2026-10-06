import { connect } from "amqplib";
import type { Channel, ChannelModel, ConfirmChannel } from "amqplib";

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

export interface TopicRouting {
  exchange: string;
  queue: string;
}

/**
 * Dựng một topic exchange `<name>` (durable) và một quorum queue `<name>.queue`, rồi bind queue vào exchange
 * bằng `pattern`. Topic pattern là các từ phân tách bằng dấu chấm: `*` thay đúng một từ, `#` thay không hoặc nhiều từ.
 *
 * Queue khai báo `x-queue-type: quorum` tường minh (compose đặt quorum làm mặc định, broker stock mặc định classic).
 * Quorum queue không thể exclusive hay auto-delete nên người gọi phải xóa queue và exchange khi dọn dẹp.
 */
export async function declareTopicRouting(
  channel: Channel,
  name: string,
  pattern: string,
): Promise<TopicRouting> {
  const queue = `${name}.queue`;
  await channel.assertExchange(name, "topic", { durable: true });
  await channel.assertQueue(queue, { durable: true, arguments: { "x-queue-type": "quorum" } });
  await channel.bindQueue(queue, name, pattern);
  return { exchange: name, queue };
}

/** Message mà broker trả về publisher qua `basic.return` (xảy ra khi publish mandatory mà không route được). */
export interface ReturnedMessage {
  replyCode: number;
  replyText: string;
  exchange: string;
  routingKey: string;
}

/**
 * Publisher ở confirm mode và biết xử lý `basic.return`.
 *
 * Broker confirm cả message không route được, nên confirm không chứng minh message đã vào queue.
 * Muốn biết message có bị bỏ hay không phải publish với `mandatory: true` và nghe sự kiện 'return'.
 * AMQP bảo đảm `basic.return` đến TRƯỚC `basic.ack` của chính message đó, nên khi confirm về
 * thì return (nếu có) đã được ghi lại. Vì vậy `publish` không cần sleep hay timeout.
 */
export class ConfirmedPublisher {
  /** Mọi message đã bị trả về, theo thứ tự đến. */
  readonly returned: ReturnedMessage[] = [];

  constructor(private readonly channel: ConfirmChannel) {
    channel.on("error", () => undefined);
    channel.on("return", (message) => {
      // File kiểu của amqplib 2.2.0 không khai báo replyCode và replyText trong MessageFields,
      // nhưng broker gửi chúng trong basic.return và test xác nhận chúng có mặt lúc chạy.
      const fields = message.fields as typeof message.fields & {
        replyCode: number;
        replyText: string;
      };
      this.returned.push({
        replyCode: fields.replyCode,
        replyText: fields.replyText,
        exchange: fields.exchange,
        routingKey: fields.routingKey,
      });
    });
  }

  /** Publish rồi chờ confirm. Resolve với message bị trả về, hoặc null nếu broker không trả gì. */
  async publish(
    exchange: string,
    routingKey: string,
    body: string,
    options: { mandatory?: boolean } = {},
  ): Promise<ReturnedMessage | null> {
    const before = this.returned.length;
    await new Promise<void>((resolve, reject) => {
      this.channel.publish(
        exchange,
        routingKey,
        Buffer.from(body),
        { mandatory: options.mandatory ?? false, persistent: true },
        (err) => (err ? reject(err) : resolve()),
      );
    });
    return this.returned.length > before ? (this.returned.at(-1) ?? null) : null;
  }
}

/** Lấy hết message đang có trong queue bằng `basic.get` (auto-ack) và trả về routing key của chúng theo thứ tự. */
export async function receiveRoutingKeys(channel: Channel, queue: string): Promise<string[]> {
  const keys: string[] = [];
  for (;;) {
    const message = await channel.get(queue, { noAck: true });
    if (message === false) return keys;
    keys.push(message.fields.routingKey);
  }
}
